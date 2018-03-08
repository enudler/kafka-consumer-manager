'use strict';

let kafka = require('kafka-node'),
    logger = require('../helpers/logger'),
    _ = require('lodash');

let previousConsumerReadOffset, offset, consumer, errorsToHealthCheck, configuration;

function init(consumerGroup, config) {
    consumer = consumerGroup;
    configuration = config;
    offset = new kafka.Offset(consumer.client);
    previousConsumerReadOffset = _.cloneDeep(consumer.topicPayloads);
}

function validateOffsetsAreSynced() {
    return new Promise((resolve, reject) => {
        if (!previousConsumerReadOffset) {
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

module.exports = {
    init,
    validateOffsetsAreSynced
};