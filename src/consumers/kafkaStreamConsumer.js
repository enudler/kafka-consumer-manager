let kafka = require('kafka-node'),
    logger = require('../helpers/logger'),
    consumerOffsetOutOfSyncChecker = require('../healthCheckers/consumerOffsetOutOfSyncChecker'),
    kafkaThrottlingManager = require('../throttling/kafkaThrottlingManager');

let configuration, consumer, shuttingDown, consumerEnabled, throttlingThreshold, throttlingCheckIntervalMs,
    isDependencyHealthy, isThirsty;

function init(config) {
    configuration = config;
    isDependencyHealthy = true;
    isThirsty = true;
    throttlingThreshold = config.ThrottlingThreshold;
    throttlingCheckIntervalMs = config.ThrottlingCheckIntervalMs;

    let options = {
        autoCommit: false,
        groupId: configuration.GroupId,
        sessionTimeout: 10000,
        protocol: ['roundrobin'],
        encoding: 'utf8',
        fetchMaxBytes: configuration.FetchMaxBytes || 1024 * 1024
    };
    if (config.KafkaUrl) {
        options['kafkaHost'] = configuration.KafkaUrl;
    } else {
        options['host'] = configuration.ZookeeperUrl;
    }
    kafkaThrottlingManager.init(throttlingThreshold, throttlingCheckIntervalMs, configuration.Topics, configuration.MessageFunction, commit);

    consumer = new kafka.ConsumerGroupStream(options, configuration.Topics);
    consumerOffsetOutOfSyncChecker.init(consumer.consumerGroup, config);
    consumerEnabled = true;

    consumer.on('data', (message) => {
        logger.trace(`consumerGroupStream got message: topic: ${message.topic}, partition: ${message.partition}, offset: ${message.offset}`);
        kafkaThrottlingManager.handleIncomingMessage(message);
    });

    consumer.on('error', (err) => {
        logger.error(err, 'Kafka Error');
    });

    consumer.on('close', () => {
        logger.info('Inner ConsumerGroupStream closed');
    });
    return Promise.resolve();
}

function validateOffsetsAreSynced() {
    if (!consumerEnabled) {
        logger.info('Monitor Offset: Skipping check as the consumer is paused');
        return Promise.resolve();
    }
    return consumerOffsetOutOfSyncChecker.validateOffsetsAreSynced();
}

function commit(message) {
    consumer.commit(message, true);
}

function closeConnection() {
    shuttingDown = true;
    logger.info('Consumer is closing connection');
    consumer.close(function (err) {
        if (err) {
            logger.error('Error when trying to close connection with kafka', {
                errorMessage: err.message
            });
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

function decreaseMessageInMemory() {
    logger.warn('Not supported for autoCommit: false');
}

module.exports = {
    init,
    pause,
    resume,
    closeConnection,
    commit,
    validateOffsetsAreSynced,
    decreaseMessageInMemory,
    setDependencyHealthy,
    setThirsty
};