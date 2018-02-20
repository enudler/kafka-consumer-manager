'use strict';
let writeBackDelay = 100,
    kafka = require('kafka-node'),
    _ = require('lodash'),
    logger = require('./logger'),
    HighLevelProducer = kafka.HighLevelProducer,
    configuration, producer, client, timeOutPromise, successPromise;

function init(config) {
    configuration = config;
    client = new kafka.KafkaClient({kafkaHost: configuration.KafkaUrl});
    producer = new HighLevelProducer(client, {requireAcks: 1});

    successPromise = new Promise((resolve) => {
        producer.on('ready', function () {
            logger.info('Producer is ready');
            resolve();
        });
    });

    timeOutPromise = new Promise((resolve, reject) => {
        setTimeout(() => {
            reject(new Error('Failed to connect to kafka after ' + configuration.KafkaConnectionTimeout + ' ms.'));
        }, configuration.KafkaConnectionTimeout);
    });

    return Promise.race([
        successPromise,
        timeOutPromise
    ]);
}

function send(message, topic) {
    return new Promise((resolve, reject) => {
        setTimeout(function () {
            logger.trace(`Producing message, to topic ${topic}`);

            let payloads = [{
                topic: topic,
                messages: [message]
            }];

            logger.trace('Writing message back to Kafka', payloads);
            producer.send(payloads, function (error, res) {
                if (error) {
                    logger.error('Failed to write message to Kafka: ' + message, error);
                    return reject(error);
                } else {
                    logger.trace('message written back to Kafka ' + JSON.stringify(res));
                    return resolve();
                }
            });
        }, writeBackDelay);
    });
}

module.exports = {
    init: init,
    send: send
};