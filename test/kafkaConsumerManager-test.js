'use strict';

let sinon = require('sinon');

describe('Verify mandatory params', () => {
    let sandbox;
    let kafkaConsumerManager = require('../src/kafkaConsumerManager');
    let producer = require('../src/kafkaProducer');
    let consumer = require('../src/kafkaConsumer');
    let healthChecker = require('../src/healthChecker');

    let fullConfiguration = {
        KafkaUrl: 'url',
        GroupId: 'GroupId',
        KafkaConnectionTimeout: '1000',
        KafkaOffsetDiffThreshold: '3',
        Topics: ['topic-a', 'topic-b'],
        ResumePauseIntervalMs: 100

    };

    beforeEach(() => {
        fullConfiguration.MessageFunction = (msg) => {
        };

        fullConfiguration.ResumePauseCheckFunction = () => {
        };
    });

    before(() => {
        sandbox = sinon.sandbox.create();
        let producerInitStub = sandbox.stub(producer, 'init');
        let consumerInitStub = sandbox.stub(consumer, 'init');
        let healthCheckerInitStub = sandbox.stub(healthChecker, 'init');

        producerInitStub.resolves();
        consumerInitStub.resolves();
        healthCheckerInitStub.resolves();

    });

    after(() => {
        sandbox.restore();
    });

    it('All params exists', async () => {
        await kafkaConsumerManager.init(fullConfiguration);
    });

    it('All params are missing', async () => {
        let config = {};

        try {
            await kafkaConsumerManager.init(config, () => {
            });
            throw new Error('Should fail');
        } catch (err) {
            err.message.should.eql('Missing mandatory environment variables: KafkaUrl,GroupId,KafkaOffsetDiffThreshold,KafkaConnectionTimeout,Topics,ResumePauseIntervalMs');
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
