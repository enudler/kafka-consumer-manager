'use strict';
let kafka = require('kafka-node'),
    logger = require('../helpers/logger');

module.exports = class KafkaProducer {
    init(config) {
        let {KafkaConnectionTimeout = 10000, KafkaUrl, WriteBackDelay = 0 } = config
        let client = new kafka.KafkaClient({kafkaHost: KafkaUrl,
            connectTimeout: KafkaConnectionTimeout});

        Object.assign(this, {
            producer : new kafka.HighLevelProducer(client, {requireAcks: 1}),
            writeBackDelay : WriteBackDelay
        });

        return new Promise((resolve, reject) => {
            this.producer.on('ready', function () {
                logger.info('Producer is ready');
                return resolve();
            });

            setTimeout(function() {
                return reject(new Error(`Failed to connect to kafka producer after ${KafkaConnectionTimeout} ms.`));
            }, KafkaConnectionTimeout);
        });
    }

    send(message, topic) {
        return new Promise((resolve, reject) => {
            setTimeout(function () {
                logger.trace(`Producing message, to topic ${topic}`);

                let payloads = [{
                    topic: topic,
                    messages: [message]
                }];

                logger.trace('Writing message back to Kafka', payloads);
                this.producer.send(payloads, function (error, res) {
                    if (error) {
                        logger.error('Failed to write message to Kafka: ' + message, error);
                        return reject(error);
                    } else {
                        logger.trace('message written back to Kafka ' + JSON.stringify(res));
                        return resolve();
                    }
                });
            }.bind(this), this.writeBackDelay);
        });
    }
}
