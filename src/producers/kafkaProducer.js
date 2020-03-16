'use strict';
let kafka = require('kafka-node');

module.exports = class KafkaProducer {
    init(config, logger) {
        let {KafkaConnectionTimeout = 10000, KafkaUrl, WriteBackDelay = 0 } = config
        let client = new kafka.KafkaClient({ kafkaHost: KafkaUrl,
            connectTimeout: KafkaConnectionTimeout });

        Object.assign(this, {
            logger: logger,
            producer: new kafka.HighLevelProducer(client, { requireAcks: 1 }),
            writeBackDelay: WriteBackDelay
        });

        return new Promise((resolve, reject) => {
            this.producer.on('ready', function () {
                this.logger.info('Producer is ready');
                return resolve();
            }.bind(this));

            setTimeout(function() {
                return reject(new Error(`Failed to connect to kafka producer after ${KafkaConnectionTimeout} ms.`));
            }, KafkaConnectionTimeout);
        });
    }

    send(message, topic) {
        return new Promise((resolve, reject) => {
            setTimeout(function () {
                this.logger.trace(`Producing message, to topic ${topic}`);

                let payloads = [{
                    topic: topic,
                    messages: [message]
                }];

                this.logger.trace('Writing message back to Kafka', payloads);
                this.producer.send(payloads, function (error, res) {
                    if (error) {
                        this.logger.error('Failed to write message to Kafka: ' + message, error);
                        return reject(error);
                    } else {
                        this.logger.trace('message written back to Kafka ' + JSON.stringify(res));
                        return resolve();
                    }
                }.bind(this));
            }.bind(this), this.writeBackDelay);
        });
    }
};
