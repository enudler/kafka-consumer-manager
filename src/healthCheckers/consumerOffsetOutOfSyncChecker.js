'use strict';

let kafka = require('kafka-node'),
    _ = require('lodash');

module.exports = class ConsumerOffsetOutOfSyncChecker {
    init(consumerGroup, kafkaOffsetDiffThreshold, logger) {
        Object.assign(this, {
            logger: logger,
            consumer: consumerGroup,
            kafkaOffsetDiffThreshold: kafkaOffsetDiffThreshold,
            offset: new kafka.Offset(consumerGroup.client),
            previousConsumerReadOffset: _.cloneDeep(consumerGroup.topicPayloads)
        });
    }

    validateOffsetsAreSynced() {
        return new Promise((resolve, reject) => {
            if (!this.previousConsumerReadOffset) {
                this.logger.info('Monitor Offset: Skipping check as the consumer is not ready');
                return resolve();
            }

            let notIncrementedTopicPayloads = getNotIncrementedTopicPayloads(this.previousConsumerReadOffset, this.consumer);

            if (notIncrementedTopicPayloads.length === 0) {
                this.logger.trace('Monitor Offset: Skipping check as offsets change was detected in the consumer', {
                    previous: this.previousConsumerReadOffset,
                    current: this.consumer.topicPayloads
                });
                this.previousConsumerReadOffset = _.cloneDeep(this.consumer.topicPayloads);
                return resolve();
            } else {
                let offsetsPayload = buildOffsetRequestPayloads(notIncrementedTopicPayloads);
                this.logger.trace('Monitor Offset: No progress detected in offsets in all partitions since the last check. Checking that the consumer is in sync..');
                if (this.consumer.topicPayloads.length > 0) {
                    this.offset.fetch(offsetsPayload, function (err, zookeeperOffsets) {
                        if (err) {
                            this.logger.error(err, 'Monitor Offset: Failed to fetch offsets');
                            return reject(new Error('Monitor Offset: Failed to fetch offsets:' + err.message));
                        }

                        let errorsToHealthCheck = isOffsetsInSync(notIncrementedTopicPayloads, zookeeperOffsets,
                            this.kafkaOffsetDiffThreshold, this.logger);
                        if (errorsToHealthCheck) {
                            return reject(errorsToHealthCheck);
                        } else {
                            this.logger.trace('Monitor Offset: Consumer found to be in sync', this.consumer.topicPayloads);
                            this.previousConsumerReadOffset = _.cloneDeep(this.consumer.topicPayloads);
                            return resolve();
                        }
                    }.bind(this));
                } else {
                    return reject(new Error('Kafka consumer Payloads are empty.'));
                }
            }
        });
    }

    async registerOffsetGauge(kafkaConsumerGroupOffset) {
        let offsetUpdate = async () => {
            let currentOffsetArr = await getOffsetsArray(this.consumer, this.offset, this.logger);
            if (currentOffsetArr) {
                currentOffsetArr.forEach((offsetObj) => {
                    kafkaConsumerGroupOffset.set({
                        topic: offsetObj.topic,
                        partition: offsetObj.partition
                    }, offsetObj.offset);
                });
            }
        };
        setInterval(offsetUpdate, 5000).unref();
    }
};

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
function getNotIncrementedTopicPayloads(previousConsumerReadOffset, consumer) {
    let notIncrementedTopicPayloads = consumer.topicPayloads.filter((topicPayload) => {
        let {topic, partition, offset: currentOffset} = topicPayload;
        let previousTopicPayloadForPartition = _.find(previousConsumerReadOffset, {topic, partition});
        return previousTopicPayloadForPartition && currentOffset === previousTopicPayloadForPartition.offset;
    });

    return notIncrementedTopicPayloads;
}

// Compares the consumer offsets vs ZooKeeper's offset
// Will return false if founds a diff
function isOffsetsInSync(notIncrementedTopicPayloads, zookeeperOffsets, kafkaOffsetDiffThreshold, logger) {
    logger.trace('Monitor Offset: Topics offsets', zookeeperOffsets);
    let lastErrorToHealthCheck;
    notIncrementedTopicPayloads.forEach(function (topicPayload) {
        let {topic, partition, offset} = topicPayload;

        if (zookeeperOffsets && zookeeperOffsets[topic] && zookeeperOffsets[topic][partition]) {
            let zkLatestOffset = zookeeperOffsets[topic][partition][0];
            let unhandledMessages = zkLatestOffset - offset;
            if (unhandledMessages > kafkaOffsetDiffThreshold) {
                let state = {
                    topic: topic,
                    partition: partition,
                    partitionLatestOffset: zkLatestOffset,
                    partitionReadOffset: offset,
                    unhandledMessages: unhandledMessages
                };

                logger.error('Monitor Offset: Kafka consumer offsets found to be out of sync', state);
                lastErrorToHealthCheck = new Error('Monitor Offset: Kafka consumer offsets found to be out of sync:' + JSON.stringify(state));
            }
        } else {
            logger.error('Monitor Offset: Kafka consumer topics/partitions found to be out of sync in topic: ' + topic + ' and in partition:' + partition);
            lastErrorToHealthCheck = new Error('Monitor Offset: Kafka consumer topics/partitions found to be out of sync in topic: ' + topic + ' and in partition:' + partition);
        }
    });

    return lastErrorToHealthCheck;
}

async function getOffsetsArray(consumer, offset, logger) {
    if (consumer.topicPayloads && consumer.topicPayloads.length > 0) {
        let offsetsArr = new Array(consumer.topicPayloads.length);
        let offsetsPayloads = buildOffsetRequestPayloads(consumer.topicPayloads);
        return new Promise(function (resolve, reject) {
            offset.fetch(offsetsPayloads, function (err, zookeeperOffsets) {
                if (err) {
                    logger.error(err, 'Monitor Offset: Failed to fetch offsets');
                    return reject(err);
                }
                consumer.topicPayloads.forEach((topicPayload, i) => {
                    let {topic, partition, offset} = topicPayload;
                    if (zookeeperOffsets && zookeeperOffsets[topic] && zookeeperOffsets[topic][partition]) {
                        let zkLatestOffset = zookeeperOffsets[topic][partition][0];
                        offsetsArr[i] = {
                            topic: topic,
                            offset: zkLatestOffset - offset,
                            partition: partition
                        };
                    }
                });
                return resolve(offsetsArr);
            });
        });
    }
}