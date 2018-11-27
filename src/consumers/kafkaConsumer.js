let kafka = require('kafka-node'),
    ConsumerOffsetOutOfSyncChecker = require('../healthCheckers/consumerOffsetOutOfSyncChecker'),
    _ = require('lodash');

module.exports = class KafkaConsumer {
    init(config, logger) {
        let {FetchMaxBytes, Topics, MessageFunction, KafkaConnectionTimeout = 10000, KafkaUrl, GroupId} = config;
        return new Promise((resolve, reject) => {
            let options = {
                kafkaHost: KafkaUrl,
                autoCommit: true,
                groupId: GroupId,
                sessionTimeout: 10000,
                protocol: ['roundrobin'],
                encoding: 'utf8',
                fetchMaxBytes: FetchMaxBytes || 1024 * 1024
            };

            Object.assign(this, {
                logger: logger,
                configuration: config,
                alreadyConnected: false,
                shuttingDown: false,
                consumerEnabled: false,
                messagesInMemory: 0,
                consumer: new kafka.ConsumerGroup(options, Topics),
                isDependencyHealthy: true,
                isThirsty: true
            });

            this.consumer.on('message', function(message){
                this.lastMessage = message;
                this.increaseMessageInMemory();
                MessageFunction(message);
            }.bind(this));

            this.consumer.on('error', function (err) {
                this.logger.error(err, 'Kafka Error');
                return reject(err);
            }.bind(this));

            this.consumer.on('offsetOutOfRange', function (err) {
                this.logger.error(err, 'offsetOutOfRange Error');
            }.bind(this));

            this.consumer.on('connect', function(err){
                if (err){
                    this.logger.error('Error when trying to connect kafka', {errorMessage: err.message});
                    return reject(err);
                } else {
                    this.logger.info('Kafka client is ready');
                    this.logger.info('topicPayloads', this.consumer.topicPayloads);
                    // Consumer is ready when "connect" event is fired + consumer has topicPayload metadata
                    if (!this.alreadyConnected && this.consumer.topicPayloads.length !== 0) {
                        this.alreadyConnected = true;
                        this.consumerEnabled = true;
                        resolve();
                    }
                }
            }.bind(this));

            setTimeout(function() {
                reject(new Error(`Failed to connect to kafka after ${KafkaConnectionTimeout} ms.`));
            }, KafkaConnectionTimeout);
        }).then(() => {
            this.consumerOffsetOutOfSyncChecker = new ConsumerOffsetOutOfSyncChecker();
            this.consumerOffsetOutOfSyncChecker.init(this.consumer,
                this.configuration.KafkaOffsetDiffThreshold, this.logger);
        });
    }

    closeConnection() {
        this.shuttingDown = true;
        this.logger.info('Consumer is closing connection');
        return new Promise((resolve, reject) => {
            this.consumer.close(function (err) {
                if (err) {
                    this.logger.error('Error when trying to close connection with kafka', {errorMessage: err.message});
                    return reject(err);
                } else {
                    return resolve();
                }
            }.bind(this));
        });
    }

    validateOffsetsAreSynced() {
        if (!this.consumerEnabled) {
            this.logger.info('Monitor Offset: Skipping check as the consumer is paused');
            return Promise.resolve();
        }

        return this.consumerOffsetOutOfSyncChecker.validateOffsetsAreSynced();
    }

    pause() {
        if (this.consumerEnabled) {
            this.logger.info('Suspending Kafka consumption');
            this.consumerEnabled = false;
            this.consumer.pause();
        }
    }

    resume() {
        if (!this.isDependencyHealthy) {
            this.logger.info('Not resuming consumption because dependency check returned false');
        } else if (!this.isThirsty) {
            this.logger.info('Not resuming consumption because too many messages in memory');
        } else if (!this.shuttingDown && !this.consumerEnabled) {
            this.logger.info('Resuming Kafka consumption');
            this.consumerEnabled = true;
            this.consumer.resume();
        }
    }

    setDependencyHealthy(value) {
        this.isDependencyHealthy = value;
    }

    setThirsty(value) {
        this.isThirsty = value;
    }

    increaseMessageInMemory() {
        if (!this.configuration.MaxMessagesInMemory || !this.configuration.ResumeMaxMessagesRatio) {
            return;
        }
        this.messagesInMemory++;
        if (this.consumerEnabled && this.messagesInMemory >= this.configuration.MaxMessagesInMemory) {
            this.logger.info(`Reached ${this.messagesInMemory} messages (max is ${this.configuration.MaxMessagesInMemory}), pausing kafka consumers`);
            this.pause();
        }
    }

    decreaseMessageInMemory() {
        if (!this.configuration.MaxMessagesInMemory || !this.configuration.ResumeMaxMessagesRatio) {
            this.logger.warn('MaxMessagesInMemory and ResumeMaxMessagesRatio must have value to enable this feature');
            return;
        }
        this.messagesInMemory--;
        if (!this.consumerEnabled && this.messagesInMemory <
            this.configuration.MaxMessagesInMemory * this.configuration.ResumeMaxMessagesRatio) {
            this.logger.info(`Reached below ${this.configuration.ResumeMaxMessagesRatio}
            % of ${this.configuration.MaxMessagesInMemory} concurrent messages, resuming kafka`);
            this.resume();
        }
    }

    getLastMessage(){
        return this.lastMessage;
    }

    on(eventName, eventHandler) {
        return this.consumer.on(eventName, eventHandler);
    }
};
