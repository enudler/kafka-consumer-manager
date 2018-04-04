'use strict';

let sinon = require('sinon'),
    rewire = require('rewire'),
    should = require('should');

describe('Verify mandatory params', () => {
    let sandbox;
    let kafkaConsumerManager = rewire('../src/kafkaConsumerManager');
    let producer = require('../src/producers/kafkaProducer');
    let consumer = require('../src/consumers/kafkaConsumer');
    let healthChecker = require('../src/healthCheckers/dependencyChecker');

    let fullConfiguration = {
        KafkaUrl: 'url',
        GroupId: 'GroupId',
        KafkaConnectionTimeout: '1000',
        KafkaOffsetDiffThreshold: '3',
        Topics: ['topic-a', 'topic-b'],
        AutoCommit: true
    };

    let autoCommitFalseWithBothAddresses = {
        KafkaUrl: 'url',
        ZookeeperUrl: 'ZK',
        GroupId: 'GroupId',
        KafkaConnectionTimeout: '1000',
        KafkaOffsetDiffThreshold: '3',
        Topics: ['topic-a', 'topic-b'],
        AutoCommit: false
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
            err.message.should.eql('Missing mandatory environment variables: GroupId,KafkaOffsetDiffThreshold,KafkaConnectionTimeout,Topics,AutoCommit,KafkaUrl');
        }
    });

    it('Both consumerGroup url types exist - should fail', async () => {

        try {
            await kafkaConsumerManager.init(autoCommitFalseWithBothAddresses, () => {
            });
            throw new Error('Should fail');
        } catch (err) {
            err.message.should.eql('Only one of the following: [KafkaUrl, ZookeeperUrl] should exist');
        }
    });

    it('No consumerGroup url types exist - should fail', async () => {
        let noAddressConfig = Object.assign({}, autoCommitFalseWithBothAddresses);
        delete noAddressConfig.KafkaUrl;
        delete noAddressConfig.ZookeeperUrl;
        try {
            await kafkaConsumerManager.init(noAddressConfig, () => {
            });
            throw new Error('Should fail');
        } catch (err) {
            err.message.should.eql('Missing mandatory environment variables. one of the following: [KafkaUrl, ZookeeperUrl] should exist');
        }
    });

    it('Only Zookeeper type exist in consumerGroup config- should succeed', async () => {
        let zookeeperAddressConfig = Object.assign({}, autoCommitFalseWithBothAddresses);
        delete zookeeperAddressConfig.KafkaUrl;
        zookeeperAddressConfig.ThrottlingThreshold = 100;
        zookeeperAddressConfig.ThrottlingCheckIntervalMs = 100;
        zookeeperAddressConfig.MessageFunction = () => {};
        await kafkaConsumerManager.init(zookeeperAddressConfig, () => {
        });
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

    it('ResumePauseIntervalMs exists without the ResumePauseCheckFunction should fail', async () => {
        let fullConfigurationWithPauseResume = JSON.parse(JSON.stringify(fullConfiguration));
        fullConfigurationWithPauseResume.ResumePauseIntervalMs = 1000;
        try {
            await kafkaConsumerManager.init(fullConfigurationWithPauseResume, () => {
            });
            throw new Error('Should fail');
        } catch (err) {
            err.message.should.eql('ResumePauseCheckFunction should be a valid function');
        }
    });

    it('ResumePauseIntervalMs exists with the ResumePauseCheckFunction should success', async () => {
        let fullConfigurationWithPauseResume = JSON.parse(JSON.stringify(fullConfiguration));
        fullConfigurationWithPauseResume.ResumePauseIntervalMs = 1000;
        fullConfigurationWithPauseResume.ResumePauseCheckFunction = () => {
            return Promise.resolve();
        };
        fullConfigurationWithPauseResume.MessageFunction = () => {
            return Promise.resolve();
        };
        await kafkaConsumerManager.init(fullConfigurationWithPauseResume);
    });
});

describe('Verify export functions', () => {
    let sandbox, consumer, resumeStub, pauseStub, validateOffsetsAreSyncedStub,
        closeConnectionStub, decreaseMessageInMemoryStub, kafkaConsumerManager;

    before(() => {
        kafkaConsumerManager = rewire('../src/kafkaConsumerManager');
        sandbox = sinon.sandbox.create();
        resumeStub = sandbox.stub();
        pauseStub = sandbox.stub();
        validateOffsetsAreSyncedStub = sandbox.stub();
        closeConnectionStub = sandbox.stub();
        decreaseMessageInMemoryStub = sandbox.stub();

        let consumer = {
            resume: resumeStub,
            pause: pauseStub,
            validateOffsetsAreSynced: validateOffsetsAreSyncedStub,
            closeConnection: closeConnectionStub,
            decreaseMessageInMemory: decreaseMessageInMemoryStub
        };
        kafkaConsumerManager.__set__('chosenConsumer', consumer);
    });

    after(() => {
        sandbox.restore();
    });

    it('Verify methods going to the correct consumer', () => {
        kafkaConsumerManager.resume();
        should(resumeStub.calledOnce).eql(true);

        kafkaConsumerManager.pause();
        should(pauseStub.calledOnce).eql(true);

        kafkaConsumerManager.validateOffsetsAreSynced();
        should(validateOffsetsAreSyncedStub.calledOnce).eql(true);

        kafkaConsumerManager.closeConnection();
        should(closeConnectionStub.calledOnce).eql(true);

        kafkaConsumerManager.finishedHandlingMessage();
        should(decreaseMessageInMemoryStub.calledOnce).eql(true);
    });
});