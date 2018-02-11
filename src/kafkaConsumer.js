let kafka = require('kafka-node'),
    logger = require('./logger'),
    previousConsumerReadOffset,
    _ = require('lodash');

let configuration, consumer, offset, errorsToHealthCheck, shuttingDown, ready,
    consumerEnabled, successPromise, timeOutPromise, alreadyConnected;

let messagesInMemory = 0;

function init(config) {
    configuration = config;
    alreadyConnected = false;
    shuttingDown = false;
    consumerEnabled = true;

    successPromise = new Promise((resolve, reject) => {
        let options = {
            kafkaHost: configuration.KafkaUrl,
            autoCommit: true,
            groupId: configuration.GroupId,
            sessionTimeout: 10000,
            protocol: ['roundrobin'],
            encoding: 'utf8',
            fetchMaxBytes: configuration.FetchMaxBytes || 1024 * 1024
        };

        consumer = new kafka.ConsumerGroup(options, configuration.Topics);
        offset = new kafka.Offset(consumer.client);
        consumer.on('message', (message) => {
            increaseMessageInMemory();
            configuration.MessageFunction(message);
        });

        consumer.on('error', function (err) {
            logger.error(err, 'Kafka Error');
        });

        consumer.on('offsetOutOfRange', function (err) {
            logger.error(err, 'offsetOutOfRange Error');
        });

        consumer.on('connect', function () {
            logger.info('Kafka client is ready');
            logger.info('topicPayloads', consumer.topicPayloads);
            // Consumer is ready when "connect" event is fired + consumer has topicPayload metadata
            if (!alreadyConnected && consumer.topicPayloads.length !== 0) {
                previousConsumerReadOffset = _.cloneDeep(consumer.topicPayloads);
                alreadyConnected = true;
                ready = true;
                resolve();
            }
        });
    });

    timeOutPromise = new Promise((resolve, reject) => {
        setTimeout(() => {
            reject(new Error(`Failed to connect to kafka after ${configuration.KafkaConnectionTimeout} ms.`));
        }, configuration.KafkaConnectionTimeout);
    });

    return Promise.race([
        successPromise,
        timeOutPromise
    ]);
}

function healthCheck() {
    return new Promise((resolve, reject) => {
        // If consumer is not ready yet - do not try to check offsets
        if (!ready) {
            return resolve();
        }

        if (!consumerEnabled) {
            logger.info('Monitor Offset: Skipping check as the consumer is paused');
            return resolve();
        } else if (!previousConsumerReadOffset) {
            logger.info('Monitor Offset: Skipping check as the consumer is not ready');
            return resolve();
        }

        let notIncrementedTopicPayloads = getNotIncrementedTopicPayloads();

        if (notIncrementedTopicPayloads.length === 0) {
            logger.trace('Monitor Offset: Skipping check as offsets change was detected in the consumer', {
                previous: previousConsumerReadOffset,
                current: consumer.topicPayloads
            });
            previousConsumerReadOffset = _.cloneDeep(consumer.topicPayloads);
            return resolve();
        } else {
            let offsetsPayload = buildOffsetRequestPayloads(notIncrementedTopicPayloads);
            logger.trace('Monitor Offset: No progress detected in offsets in all partitions since the last check. Checking that the consumer is in sync..');
            if (consumer.topicPayloads.length > 0) {
                offset.fetch(offsetsPayload, function (err, zookeeperOffsets) {
                    if (err) {
                        logger.error(err, 'Monitor Offset: Failed to fetch offsets');
                        return reject(new Error('Monitor Offset: Failed to fetch offsets:' + err.message));
                    }
                    if (isOffsetsInSync(notIncrementedTopicPayloads, zookeeperOffsets)) {
                        logger.trace('Monitor Offset: Consumer found to be in sync', consumer.topicPayloads);
                        previousConsumerReadOffset = _.cloneDeep(consumer.topicPayloads);
                        return resolve();
                    } else {
                        return reject(errorsToHealthCheck);
                    }
                });
            } else {
                return reject(new Error('Kafka consumer Payloads are empty.'));
            }
        }
    });
}

function buildOffsetRequestPayloads(topicPayloads) {
    let offsetRequestPayloads = topicPayloads.map(function (topicPayload) {
        return {
            topic: topicPayload.topic,
            partition: topicPayload.partition,
            time: -1
        };
    });

    return offsetRequestPayloads;
}

// Compares the current consumer offsets vs the its previous state
// Returns topic payload that wasn't incremented from last check
function getNotIncrementedTopicPayloads() {
    let notIncrementedTopicPayloads = consumer.topicPayloads.filter((topicPayload) => {
        let {topic, partition, offset: currentOffset} = topicPayload;
        let previousTopicPayloadForPartition = _.find(previousConsumerReadOffset, {topic, partition});
        return previousTopicPayloadForPartition && currentOffset === previousTopicPayloadForPartition.offset;
    });

    return notIncrementedTopicPayloads;
}

// Compares the consumer offsets vs ZooKeeper's offset
// Will return false if founds a diff
function isOffsetsInSync(notIncrementedTopicPayloads, zookeeperOffsets) {
    logger.trace('Monitor Offset: Topics offsets', zookeeperOffsets);
    let isSynced = notIncrementedTopicPayloads.every(function (topicPayload) {
        let {topic, partition, offset} = topicPayload;

        if (zookeeperOffsets && zookeeperOffsets[topic] && zookeeperOffsets[topic][partition]) {
            let zkLatestOffset = zookeeperOffsets[topic][partition][0];
            let unhandledMessages = zkLatestOffset - offset;
            if (unhandledMessages > configuration.KafkaOffsetDiffThreshold) {
                let state = {
                    topic: topic,
                    partition: partition,
                    partitionLatestOffset: zkLatestOffset,
                    partitionReadOffset: offset,
                    unhandledMessages: unhandledMessages
                };

                logger.error('Monitor Offset: Kafka consumer offsets found to be out of sync', state);
                errorsToHealthCheck = new Error('Monitor Offset: Kafka consumer offsets found to be out of sync:' + JSON.stringify(state));
                return false;
            }
        } else {
            logger.error('Monitor Offset: Kafka consumer topics/partitions found to be out of sync in topic: ' + topic + ' and in partition:' + partition);
            errorsToHealthCheck = new Error('Monitor Offset: Kafka consumer topics/partitions found to be out of sync in topic: ' + topic + ' and in partition:' + partition);
            return false;
        }
        return true;
    });

    return isSynced;
}

function closeConnection() {
    shuttingDown = true;
    logger.info('Consumer is closing connection');
    consumer.close(function (err) {
        if (err) {
            logger.error('Error when trying to close connection with kafka', {errorMessage: err.message});
        }
    });
}

function pause() {
    if (consumerEnabled) {
        logger.info('Suspending Kafka consumption');
        consumerEnabled = false;
        consumer.pause();
    }
}

function resume() {
    if (!shuttingDown && !consumerEnabled) {
        logger.info('Resuming Kafka consumption');
        consumerEnabled = true;
        consumer.resume();
    }
}

function increaseMessageInMemory() {
    if (!configuration.MaxMessagesInMemory || !configuration.ResumeMaxMessagesRatio) {
        return;
    }
    messagesInMemory++;
    if (consumerEnabled && messagesInMemory >= configuration.MaxMessagesInMemory) {
        logger.info(`Reached ${messagesInMemory} messages (max is ${configuration.MaxMessagesInMemory}), pausing kafka consumers`);
        pause();
    }
}

function decreaseMessageInMemory() {
    if (!configuration.MaxMessagesInMemory || !configuration.ResumeMaxMessagesRatio) {
        logger.warn('MaxMessagesInMemory and ResumeMaxMessagesRatio must have value to enable this feature');
        return;
    }
    messagesInMemory--;
    if (!consumerEnabled && messagesInMemory < configuration.MaxMessagesInMemory * configuration.ResumeMaxMessagesRatio) {
        logger.info(`Reached below ${configuration.ResumeMaxMessagesRatio}% of ${configuration.MaxMessagesInMemory} concurrent messages, resuming kafka`);
        resume();
    }
}

module.exports = {
    init: init,
    healthCheck: healthCheck,
    pause: pause,
    resume: resume,
    closeConnection: closeConnection,
    decreaseMessageInMemory: decreaseMessageInMemory
};