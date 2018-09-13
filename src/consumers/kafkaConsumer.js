let kafka = require('kafka-node'),
    logger = require('../helpers/logger'),
    consumerOffsetOutOfSyncChecker = require('../healthCheckers/consumerOffsetOutOfSyncChecker'),
    _ = require('lodash');

let configuration, consumer, shuttingDown,
    consumerEnabled, successPromise, timeOutPromise, alreadyConnected, isDependencyHealthy, isThirsty,lastMessage;

let messagesInMemory = 0;

function init(config) {
    configuration = config;
    alreadyConnected = false;
    shuttingDown = false;
    consumerEnabled = false;

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
        consumer.on('message', (message) => {
            lastMessage = message;
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
                alreadyConnected = true;
                consumerEnabled = true;
                consumerOffsetOutOfSyncChecker.init(consumer, config);
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

function closeConnection() {
    shuttingDown = true;
    logger.info('Consumer is closing connection');
    consumer.close(function (err) {
        if (err) {
            logger.error('Error when trying to close connection with kafka', {errorMessage: err.message});
        }
    });
}

function validateOffsetsAreSynced() {
    if (!consumerEnabled) {
        logger.info('Monitor Offset: Skipping check as the consumer is paused');
        return Promise.resolve();
    }

    return consumerOffsetOutOfSyncChecker.validateOffsetsAreSynced();
}

function pause() {
    if (consumerEnabled) {
        logger.info('Suspending Kafka consumption');
        consumerEnabled = false;
        consumer.pause();
    }
}

function resume() {
    if (!isDependencyHealthy) {
        logger.info('Not resuming consumption because dependency check returned false');
    } else if (!isThirsty) {
        logger.info('Not resuming consumption because too many messages in memory');
    } else if (!shuttingDown && !consumerEnabled) {
        logger.info('Resuming Kafka consumption');
        consumerEnabled = true;
        consumer.resume();
    }
}

function setDependencyHealthy(value) {
    isDependencyHealthy = value;
}

function setThirsty(value) {
    isThirsty = value;
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

function getLastMessage(){
    return _.cloneDeep(lastMessage);
}

module.exports = {
    init: init,
    validateOffsetsAreSynced,
    pause,
    resume,
    closeConnection,
    decreaseMessageInMemory,
    setDependencyHealthy,
    setThirsty,
    getLastMessage
};