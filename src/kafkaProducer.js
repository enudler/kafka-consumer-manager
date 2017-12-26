'use strict';
let writeBackDelay = 100,
    kafka = require('kafka-node'),
    _ = require('lodash'),
    logger = require('./logger'),
    configuration, client, producer, timeOutPromise, successPromise;

function init(config) {
    configuration = config;
    client = new kafka.Client(configuration.ZOOKEEPER_URL);
    producer = new kafka.HighLevelProducer(client, {
        requireAcks: 1,
        encoding: 'utf8'
    });

    successPromise = new Promise((resolve) => {
        producer.on('ready', function () {
            logger.info('Producer is ready');
            resolve();
        });
    });

    timeOutPromise = new Promise((resolve, reject) => {
        setTimeout(() => {
            reject(new Error('Failed to connect to kafka after ' + configuration.KAFKA_CONNECTION_TIMEOUT + ' ms.'));
        }, configuration.KAFKA_CONNECTION_TIMEOUT);
    });

    return Promise.race([
        successPromise,
        timeOutPromise
    ]);
}

function retryMessage(failedMessage, topic) {
    setTimeout(function () {
        logger.info(`Producing retried message, to topic ${topic}`);

        let payloads = [{
            topic: topic,
            messages: [failedMessage]
        }];

        logger.trace('Writing message back to Kafka', payloads);
        producer.send(payloads, function (error, res) {
            if (error) {
                logger.error('Failed to write message to Kafka: ' + failedMessage, error);
            } else {
                logger.trace('message written back to Kafka ' + JSON.stringify(res));
            }
        });
    }, writeBackDelay);
}

module.exports = {
    init: init,
    retryMessage: retryMessage
};