let kafka = require('kafka-node'),
    logger = require('../helpers/logger'),
    ConsumerOffsetOutOfSyncChecker = require('../healthCheckers/consumerOffsetOutOfSyncChecker'),
    KafkaThrottlingManager = require('../throttling/kafkaThrottlingManager'),
    _ = require('lodash');

module.exports = class KafkaStreamConsumer {
    init(config){
        return new Promise((resolve, reject) => {
            let {KafkaUrl, GroupId, Topics, MessageFunction, FetchMaxBytes,
                AutoCommitIntervalMs, ThrottlingThreshold, ThrottlingCheckIntervalMs, KafkaConnectionTimeout = 10000} = config;

            let options = {
                kafkaHost: KafkaUrl,
                autoCommit: false,
                groupId: GroupId,
                sessionTimeout: 10000,
                protocol: ['roundrobin'],
                encoding: 'utf8',
                fetchMaxBytes: FetchMaxBytes || 1024 * 1024,
                autoCommitIntervalMs: AutoCommitIntervalMs || 5000
            };

            let consumer = new kafka.ConsumerGroupStream(options, Topics);
            Object.assign(this, {
                configuration: config,
                isDependencyHealthy: true,
                isThirsty: true,
                commitEachMessage: _.get(config, 'CommitEachMessage', true),
                consumerEnabled: true,
                consumer: consumer
            });

            this.consumer.on('data', function(message){
                this.lastMessage = message;
                logger.trace(`consumerGroupStream got message: topic: ${message.topic}, partition: ${message.partition}, offset: ${message.offset}`);
                this.kafkaThrottlingManager.handleIncomingMessage(message);
            }.bind(this));

            this.consumer.on('error', (err) => {
                logger.error(err, 'Kafka Error');
            });

            this.consumer.on('close', () => {
                logger.info('Inner ConsumerGroupStream closed');
            });

            this.consumer.on('connect', function(err){
                if (err) {
                    reject(err);
                } else {
                    logger.info('Kafka client is ready');
                    logger.info('topicPayloads', this.consumer.consumerGroup.topicPayloads);
                    this.kafkaThrottlingManager = new KafkaThrottlingManager();
                    this.kafkaThrottlingManager.init(ThrottlingThreshold, ThrottlingCheckIntervalMs,
                        Topics, MessageFunction, this);
                    this.consumerOffsetOutOfSyncChecker = new ConsumerOffsetOutOfSyncChecker(this.consumer.consumerGroup,
                        config.KafkaOffsetDiffThreshold);
                    return resolve();
                }
            }.bind(this));

            setTimeout(function() {
                return reject(new Error(`Failed to connect to kafka after ${KafkaConnectionTimeout} ms.`));
            }, KafkaConnectionTimeout);
        });
    }

    pause() {
        if (this.consumerEnabled) {
            logger.info('Suspending Kafka consumption');
            this.consumerEnabled = false;
            this.consumer.pause();
        }
    }

    resume() {
        if (!this.isDependencyHealthy) {
            logger.info('Not resuming consumption because dependency check returned false');
        } else if (!this.isThirsty) {
            logger.info('Not resuming consumption because too many messages in memory');
        } else if (!this.shuttingDown && !this.consumerEnabled) {
            logger.info('Resuming Kafka consumption');
            this.consumerEnabled = true;
            this.consumer.resume();
        }
    }

    closeConnection() {
        this.shuttingDown = true;
        logger.info('Consumer is closing connection');
        return new Promise((resolve, reject) => {
            this.consumer.close(function (err) {
                if (err) {
                    // logger.error('Error when trying to close connection with kafka', {
                    //     errorMessage: err.message
                    // });
                    return reject(err);
                } else {
                    return resolve();
                }
            });
        });
    }

    validateOffsetsAreSynced() {
        if (!this.consumerEnabled) {
            logger.info('Monitor Offset: Skipping check as the consumer is paused');
            return Promise.resolve();
        }
        return this.consumerOffsetOutOfSyncChecker.validateOffsetsAreSynced();
    }

    decreaseMessageInMemory() {
        logger.warn('Not supported for autoCommit: false');
    }

    setDependencyHealthy(value) {
        this.isDependencyHealthy = value;
    }

    setThirsty(value) {
        this.isThirsty = value;
    }

    getLastMessage(){
        return _.cloneDeep(this.lastMessage);
    }

    commit(message) {
        this.consumer.commit(message, this.commitEachMessage);
    }
};
