let kafka = require('kafka-node'),
    logger = require('./logger'),
    _ = require('lodash'),
    throttlingQueue = require('./throttlingInternalQueues'),
    kafkaThrottlingManager = require('./kafkaFlowManager');

let configuration, consumer, shuttingDown, flowManagerInterval, consumerEnabled, throttlingThreshold;

function init(config) {
    configuration = config;
    flowManagerInterval = config.flowManagerInterval;
    throttlingThreshold = config.throttlingThreshold;

    kafkaThrottlingManager.init(throttlingThreshold, flowManagerInterval);
    throttlingQueue.init(configuration.MessageFunction, commit);

    let options = {
        kafkaHost: configuration.KafkaUrl,
        autoCommit: false,
        groupId: configuration.GroupId,
        sessionTimeout: 10000,
        protocol: ['roundrobin'],
        encoding: 'utf8',
        fetchMaxBytes: configuration.FetchMaxBytes || 1024 * 1024
    };

    consumer = new kafka.ConsumerGroupStream(options, configuration.Topics);
    consumerEnabled = true;

    consumer.on('data', (message) => {
        logger.trace(`consumerGroupStream got message: topic: ${message.topic}, partition: ${message.partition}, offset: ${message.offset}`);
        throttlingQueue.handleIncomingMessage(message);
    });
    consumer.on('error', (err) => {
        logger.error(err, 'Kafka Error');
    });
    consumer.on('close', () => {
        logger.info('Inner ConsumerGroupStream closed');
    });
    return Promise.resolve();
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
    if (!shuttingDown && !consumerEnabled) {
        logger.info('Resuming Kafka consumption');
        consumerEnabled = true;
        consumer.resume();
    }
}

function healthCheck() {
    logger.warn('health check for consumerGroupStream is not implemeted yet');
    return Promise.resolve();
}

module.exports = {
    init,
    pause,
    resume,
    closeConnection,
    commit,
    healthCheck
};