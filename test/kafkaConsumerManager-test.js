'use strict';

let sinon = require('sinon');

describe('Verify mandatory params', () => {
    let sandbox;
    let kafkaConsumerManager = require('../src/kafkaConsumerManager');
    let producer = require('../src/kafkaProducer');
    let consumer = require('../src/kafkaConsumer');
    let healthChecker = require('../src/healthChecker');

    let fullConfiguration = {
        KAFKA_URL: 'url',
        GROUP_ID: 'group_id',
        KAFKA_CONNECTION_TIMEOUT: '1000',
        ZOOKEEPER_URL: 'zookeeper_url',
        KAFKA_OFFSET_DIFF_THRESHOLD: '3',
        TOPICS: ['topic-a', 'topic-b'],
        RESUME_PAUSE_INTERVAL_MS: 100

    };

    beforeEach(() => {
        fullConfiguration.MESSAGE_FUNCTION = (msg) => {
        };

        fullConfiguration.RESUME_PAUSE_CHECK_FUNCTION = () => {
        };
    });

    before(() => {
        sandbox = sinon.sandbox.create();
        sandbox.stub(producer, 'init');
        sandbox.stub(consumer, 'init');
        sandbox.stub(healthChecker, 'init');
    });

    after(() => {
        sandbox.restore();
    });

    it('All params exists', async () => {
        await kafkaConsumerManager.init(fullConfiguration, () => {
        });
    });

    it('All params are missing', async () => {
        let config = {};

        try {
            await kafkaConsumerManager.init(config, () => {
            });
            throw new Error('Should fail');
        } catch (err) {
            err.message.should.eql('Missing mandatory environment variables: KAFKA_URL,GROUP_ID,ZOOKEEPER_URL,KAFKA_OFFSET_DIFF_THRESHOLD,KAFKA_CONNECTION_TIMEOUT,TOPICS,RESUME_PAUSE_INTERVAL_MS');
        }
    });

    Object.keys(fullConfiguration).forEach(key => {
        it('Test without ' + key + ' param', async () => {
            let clonedConfig = JSON.parse(JSON.stringify(fullConfiguration));
            delete clonedConfig[key];

            try {
                await kafkaConsumerManager.init(clonedConfig, () => {
                });
                throw new Error('Should fail');
            } catch (err) {
                if (key.indexOf('FUNCTION') > -1) {
                    err.message.should.eql(key + ' should be a valid function');
                } else {
                    err.message.should.eql('Missing mandatory environment variables: ' + key);
                }
            }
        });
    });
});
