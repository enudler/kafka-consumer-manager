'use strict';

let kafka = require('kafka-node'),
    sinon = require('sinon'),
    KafkaThrottlingManager = require('../src/throttling/kafkaThrottlingManager'),
    should = require('should'),
    KafkaStreamConsumer = require('../src/consumers/kafkaStreamConsumer'),
    assert = require('assert'),
    ConsumerOffsetOutOfSyncCheckerStub = require('../src/healthCheckers/consumerOffsetOutOfSyncChecker'),
    _ = require('lodash'),
    prometheusConfig = require('../src/prometheus/prometheus-config');

let sandbox, logErrorStub, logTraceStub, logInfoStub, consumerGroupStreamStub,
    consumerStreamStub, consumerEventHandlers, resumeStub, pauseStub, consumer,
    promiseActionSpy, baseConfiguration, handleIncomingMessageStub, commitStub,
    validateOffsetsAreSyncedStub, logger, kafkaThrottlingManagerStub, offsetOutOfSyncCheckerStub;

describe('Testing events method', function () {
    before(function () {
        sandbox = sinon.sandbox.create();
        logErrorStub = sandbox.stub();
        logInfoStub = sandbox.stub();
        logTraceStub = sandbox.stub();
        logger = {error: logErrorStub, trace: logTraceStub, info: logInfoStub};
        offsetOutOfSyncCheckerStub = sandbox.stub(ConsumerOffsetOutOfSyncCheckerStub.prototype, 'init');
        handleIncomingMessageStub = sandbox.stub(KafkaThrottlingManager.prototype, 'handleIncomingMessage');
        consumerGroupStreamStub = sandbox.stub(kafka, 'ConsumerGroupStream');
        sandbox.stub(kafka, 'Offset').returns({});
        kafkaThrottlingManagerStub = sandbox.stub(KafkaThrottlingManager.prototype, 'init');
    });
    beforeEach(function () {
        consumerEventHandlers = {};

        consumerStreamStub = {
            on: function (name, func) {
                consumerEventHandlers[name] = func;
            },
            consumerGroup: {}
        };

        consumerGroupStreamStub.returns(consumerStreamStub);

        consumer = new KafkaStreamConsumer();
        promiseActionSpy = sinon.spy();

        baseConfiguration = {
            KafkaUrl: 'KafkaUrl',
            GroupId: 'GroupId',
            ThrottlingThreshold: 1000,
            ThrottlingCheckIntervalMs: 10000,
            Topics: ['topic-a', 'topic-b'],
            MessageFunction: promiseActionSpy,
            FetchMaxBytes: 128
        };
    });
    after(function () {
        sandbox.restore();
    });

    afterEach(function () {
        sandbox.reset();
    });

    describe(' on connect event', function () {
        it('fail to connect - connection error', function () {
            setTimeout(() => {
                consumerEventHandlers.connect(new Error('fail to connect'));
            }, 100);

            return consumer.init(baseConfiguration, logger).should.be.rejectedWith(new Error('fail to connect')).then(() => {
                should(kafkaThrottlingManagerStub.callCount).eql(0);
                should(offsetOutOfSyncCheckerStub.callCount).eql(0);
                should(logErrorStub.args[0]).eql(['Error when trying to connect kafka', {
                    'errorMessage': 'fail to connect'
                }]);
            });
        });

        it('fail to connect - error event', function () {
            let err = new Error('fail to connect');
            setTimeout(() => {
                consumerEventHandlers.error(err);
            }, 100);

            return consumer.init(baseConfiguration, logger).should.be.rejectedWith(new Error('fail to connect')).then(() => {
                should(kafkaThrottlingManagerStub.callCount).eql(0);
                should(offsetOutOfSyncCheckerStub.callCount).eql(0);
                should(logErrorStub.args[0]).eql([err, 'Kafka Error']);
            });
        });

        it('fail to connect - timeout', async function () {
            let baseConfiguration = {
                KafkaUrl: 'KafkaUrl',
                GroupId: 'GroupId',
                ThrottlingThreshold: 1000,
                ThrottlingCheckIntervalMs: 10000,
                Topics: ['topic-a', 'topic-b'],
                MessageFunction: () => {
                },
                KafkaConnectionTimeout: 1000
            };
            try {
                await consumer.init(baseConfiguration, logger);
                assert.fail('consumer connect should fail');
            } catch (err) {
                should(err.message).equal('Failed to connect to kafka after 1000 ms.');
                should(kafkaThrottlingManagerStub.callCount).eql(0);
                should(offsetOutOfSyncCheckerStub.callCount).eql(0);
            }
        });

        it('testing right configuration was called, full configuration', async function () {
            let commitEachMsgConfiguration = {
                KafkaUrl: 'KafkaUrl',
                GroupId: 'GroupId',
                ThrottlingThreshold: 1000,
                ThrottlingCheckIntervalMs: 10000,
                Topics: ['topic-a', 'topic-b'],
                MessageFunction: promiseActionSpy,
                FetchMaxBytes: 128,
                CommitEachMessage: false,
                AutoCommitIntervalMs: 7000
            };

            setTimeout(() => {
                consumerEventHandlers.connect();
            }, 100);
            await consumer.init(commitEachMsgConfiguration, logger);

            let optionsExpected = {
                'autoCommit': false,
                'encoding': 'utf8',
                'groupId': 'GroupId',
                'protocol': [
                    'roundrobin'
                ],
                'sessionTimeout': 10000,
                'kafkaHost': 'KafkaUrl',
                'fetchMaxBytes': 128,
                'autoCommitIntervalMs': 7000
            };

            should(consumerGroupStreamStub.args[0][1]).eql(['topic-a', 'topic-b']);
            should(consumerGroupStreamStub.args[0][0]).eql(optionsExpected);
            should(logInfoStub.args[0]).eql(['Kafka client is ready']);
            should(kafkaThrottlingManagerStub.callCount).eql(1);
            should(offsetOutOfSyncCheckerStub.callCount).eql(1);
        });

        it('testing right configuration was called, only mandatory configuration', async function () {
            let baseConfiguration = {
                KafkaUrl: 'KafkaUrl',
                GroupId: 'GroupId',
                ThrottlingThreshold: 1000,
                ThrottlingCheckIntervalMs: 10000,
                Topics: ['topic-a', 'topic-b'],
                MessageFunction: () => {
                }
            };
            setTimeout(() => {
                consumerEventHandlers.connect();
            }, 100);
            await consumer.init(baseConfiguration, logger);

            let optionsExpected = {
                'autoCommit': false,
                'encoding': 'utf8',
                'groupId': 'GroupId',
                'protocol': [
                    'roundrobin'
                ],
                'sessionTimeout': 10000,
                'kafkaHost': 'KafkaUrl',
                'fetchMaxBytes': 1048576,
                'autoCommitIntervalMs': 5000

            };

            should(consumerGroupStreamStub.args[0][1]).eql(['topic-a', 'topic-b']);
            should(consumerGroupStreamStub.args[0][0]).eql(optionsExpected);
            should(logInfoStub.args[0]).eql(['Kafka client is ready']);
            should(kafkaThrottlingManagerStub.callCount).eql(1);
            should(offsetOutOfSyncCheckerStub.callCount).eql(1);
        });
    });

    describe(' on data, error and close events', function () {
        beforeEach(async () => {
            setTimeout(() => {
                consumerEventHandlers.connect();
            }, 100);
            await consumer.init(baseConfiguration, logger);
        });

        it('testing listening functions - on data', async function () {
            let msg = {
                value: 'some_value',
                partition: 123,
                offset: 5,
                topic: 'my_topic'
            };
            consumerEventHandlers.data(msg);
            should(consumer.getLastMessage()).deepEqual(msg);
            sinon.assert.calledOnce(logTraceStub);
            sinon.assert.calledWithExactly(logTraceStub, 'consumerGroupStream got message: topic: my_topic, partition: 123, offset: 5');
            sinon.assert.calledOnce(handleIncomingMessageStub);
            sinon.assert.calledWithExactly(handleIncomingMessageStub, msg, undefined);
        });
        it('testing listening functions - on error', async function () {
            let err = {
                message: 'some_error_message',
                stack: 'some_error_stack'
            };
            consumerEventHandlers.error(err);
            sinon.assert.calledOnce(logErrorStub);
            sinon.assert.calledWithExactly(logErrorStub, err, 'Kafka Error');
        });
        it('testing listening functions - on close', async function () {
            consumerEventHandlers.close();
            sinon.assert.calledWithExactly(logInfoStub, 'Inner ConsumerGroupStream closed');
        });
    });
});

describe('Testing commit, pause and resume  methods', function () {
    let offsetStub;
    before(() => {
        sandbox = sinon.sandbox.create();
        logInfoStub = sandbox.stub();
        logger = {error: sandbox.stub(), trace: sandbox.stub(), info: logInfoStub};
        handleIncomingMessageStub = sandbox.stub(KafkaThrottlingManager.prototype, 'handleIncomingMessage');
        sandbox.stub(KafkaThrottlingManager.prototype, 'init');
        pauseStub = sandbox.stub();
        resumeStub = sandbox.stub();
        commitStub = sandbox.stub();

        offsetStub = sandbox.stub(kafka, 'Offset');

        consumerGroupStreamStub = sandbox.stub(kafka, 'ConsumerGroupStream');
    });
    beforeEach(async function () {
        consumerEventHandlers = {};

        consumerStreamStub = {
            on: function (name, func) {
                consumerEventHandlers[name] = func;
            },
            pause: pauseStub,
            resume: resumeStub,
            commit: commitStub,
            consumerGroup: {}
        };

        consumerGroupStreamStub.returns(consumerStreamStub);
        offsetStub.returns({});

        consumer = new KafkaStreamConsumer();

        promiseActionSpy = sinon.spy();

        baseConfiguration = {
            KafkaUrl: 'KafkaUrl',
            GroupId: 'GroupId',
            flowManagerInterval: 5555,
            throttlingThreshold: 555,
            Topics: ['topic-a', 'topic-b'],
            MessageFunction: promiseActionSpy,
            FetchMaxBytes: 9999
        };
    });
    afterEach(function () {
        sandbox.reset();
    });

    after(function () {
        sandbox.restore();
    });

    it('testing resume function handling - too many messages in memory', async function () {
        setTimeout(() => {
            consumerEventHandlers.connect();
        }, 150);
        await consumer.init(baseConfiguration, logger);
        consumer.setThirsty(false);
        consumer.setDependencyHealthy(true);
        consumer.resume();
        should(logInfoStub.args[2][0]).eql('Not resuming consumption because too many messages in memory');
        should(resumeStub.calledOnce).eql(false);
    });

    it('Testing resume function handling - dependency not healthy', async function () {
        setTimeout(() => {
            consumerEventHandlers.connect();
        }, 150);
        await consumer.init(baseConfiguration, logger);
        consumer.setThirsty(true);
        consumer.setDependencyHealthy(false);
        consumer.resume();
        should(logInfoStub.args[2][0]).eql('Not resuming consumption because dependency check returned false');
        should(resumeStub.calledOnce).eql(false);
    });

    it('testing pause & resume methods', async function () {
        setTimeout(() => {
            consumerEventHandlers.connect();
        }, 150);
        await consumer.init(baseConfiguration, logger);
        consumerGroupStreamStub.returns(consumerStreamStub);
        consumer.pause();
        sinon.assert.calledWithExactly(logInfoStub, 'Suspending Kafka consumption');
        sinon.assert.calledOnce(pauseStub);
        consumer.resume();
        should(logInfoStub.args[3][0]).eql('Resuming Kafka consumption');
        should(logInfoStub.callCount).eql(4);
        sinon.assert.calledOnce(resumeStub);
    });

    it('testing commit methods - CommitEachMessage is true', async function () {
        setTimeout(() => {
            consumerEventHandlers.connect();
        }, 150);
        await consumer.init(baseConfiguration, logger);
        let msg = {
            value: 'some_value',
            partition: 123,
            offset: 5,
            topic: 'my_topic'
        };
        consumerGroupStreamStub.returns(consumerStreamStub);
        consumer.commit(msg);
        sinon.assert.calledOnce(commitStub);
        sinon.assert.calledWithExactly(commitStub, msg, true);
    });

    it('testing commit methods - CommitEachMessage is false', async function () {
        let commitEachMsgConfiguration = {
            KafkaUrl: 'KafkaUrl',
            GroupId: 'GroupId',
            ThrottlingThreshold: 1000,
            ThrottlingCheckIntervalMs: 10000,
            Topics: ['topic-a', 'topic-b'],
            MessageFunction: promiseActionSpy,
            FetchMaxBytes: 128,
            CommitEachMessage: false,
            AutoCommitIntervalMs: 7000
        };

        let msg = {
            value: 'some_value',
            partition: 123,
            offset: 5,
            topic: 'my_topic'
        };

        setTimeout(() => {
            consumerEventHandlers.connect();
        }, 150);
        await consumer.init(commitEachMsgConfiguration, logger);

        consumerGroupStreamStub.returns(consumerStreamStub);
        consumer.commit(msg);
        sinon.assert.calledOnce(commitStub);
        sinon.assert.calledWithExactly(commitStub, msg, false);
    });
});

describe('testing validateOffsetsAreSynced methods', function () {
    let offsetStub;
    before(() => {
        sandbox = sinon.sandbox.create();
        logInfoStub = sandbox.stub();
        logger = {error: sandbox.stub(), trace: sandbox.stub(), info: logInfoStub};
        sandbox.stub(KafkaThrottlingManager.prototype, 'init');
        offsetStub = sandbox.stub(kafka, 'Offset');
        consumerGroupStreamStub = sandbox.stub(kafka, 'ConsumerGroupStream');
    });

    beforeEach(async function () {
        consumerEventHandlers = {};

        consumerStreamStub = {
            on: function (name, func) {
                consumerEventHandlers[name] = func;
            },
            pause: pauseStub,
            resume: resumeStub,
            commit: commitStub,
            consumerGroup: {}
        };

        consumerGroupStreamStub.returns(consumerStreamStub);
        consumer = new KafkaStreamConsumer();

        offsetStub.returns({});

        baseConfiguration = {
            KafkaUrl: 'KafkaUrl',
            GroupId: 'GroupId',
            ThrottlingThreshold: 1000,
            ThrottlingCheckIntervalMs: 10000,
            Topics: ['topic-a', 'topic-b'],
            MessageFunction: () => {
            },
            KafkaConnectionTimeout: 1000
        };

        setTimeout(() => {
            consumerEventHandlers.connect();
        }, 100);
        await consumer.init(baseConfiguration, logger);

        validateOffsetsAreSyncedStub = sandbox.stub(consumer.consumerOffsetOutOfSyncChecker,
            'validateOffsetsAreSynced').resolves();
    });
    after(function () {
        sandbox.restore();
    });

    afterEach(function () {
        sandbox.reset();
    });

    it('validateOffsetsAreSynced is not called since no consumer is not enabled yet', function () {
        consumer.consumerEnabled = false;
        return consumer.validateOffsetsAreSynced()
            .then(() => {
                should(logInfoStub.args[2][0]).eql('Monitor Offset: Skipping check as the consumer is paused');
                sinon.assert.callCount(validateOffsetsAreSyncedStub, 0);
            });
    });

    it('validateOffsetsAreSynced is called since consumer is enabled', function () {
        consumer.consumerEnabled = true;
        return consumer.validateOffsetsAreSynced()
            .then(() => {
                sinon.assert.callCount(validateOffsetsAreSyncedStub, 1);
            });
    });
});

describe('Testing closeConnection method', function () {
    before(() => {
        sandbox = sinon.sandbox.create();
        logInfoStub = sandbox.stub();
        logErrorStub = sandbox.stub();
        logger = {error: logErrorStub, trace: sandbox.stub(), info: logInfoStub};
        handleIncomingMessageStub = sandbox.stub(KafkaThrottlingManager.prototype, 'handleIncomingMessage');
        sandbox.stub(KafkaThrottlingManager.prototype, 'init');
        sandbox.stub(kafka, 'Offset');
        consumerGroupStreamStub = sandbox.stub(kafka, 'ConsumerGroupStream');
    });

    beforeEach(async function () {
        consumerEventHandlers = {};
        consumerStreamStub = {
            on: function (name, func) {
                consumerEventHandlers[name] = func;
            },
            close: () => {
            },
            consumerGroup: {},
            pause: () => {
            }
        };

        consumerGroupStreamStub.returns(consumerStreamStub);

        consumer = new KafkaStreamConsumer();

        promiseActionSpy = sinon.spy();

        baseConfiguration = {
            KafkaUrl: 'KafkaUrl',
            GroupId: 'GroupId',
            flowManagerInterval: 5555,
            throttlingThreshold: 555,
            Topics: ['topic-a', 'topic-b'],
            MessageFunction: promiseActionSpy,
            FetchMaxBytes: 9999
        };

        setTimeout(() => {
            consumerEventHandlers.connect();
        }, 100);
        await consumer.init(baseConfiguration, logger);
    });

    after(function () {
        sandbox.restore();
    });

    afterEach(function () {
        sandbox.reset();
    });

    it('Testing closeConnection method - successful closure', async function () {
        consumerStreamStub.close = (cb) => {
            cb();
        };

        consumerGroupStreamStub.returns(consumerStreamStub);
        await consumer.closeConnection();
        should(logInfoStub.callCount).equal(3);
        sinon.assert.calledWithExactly(logInfoStub, 'Consumer is closing connection');
    });
    it('Testing closeConnection method - failure in closure', async function () {
        let error = {
            message: 'error message'
        };
        consumerStreamStub.close = (cb) => {
            cb(_.cloneDeep(error));
        };
        consumerGroupStreamStub.returns(consumerStreamStub);

        return consumer.closeConnection().should.be.rejectedWith(error).then(() => {
            should(logErrorStub.args[0]).eql(['Error when trying to close connection with kafka', {
                'errorMessage': 'error message'
            }]);
        });
    });
});


describe('Testing metrics feature', function () {
    before(() => {
        sandbox = sinon.sandbox.create();
        logInfoStub = sandbox.stub();
        logErrorStub = sandbox.stub();
        logger = {error: logErrorStub, trace: sandbox.stub(), info: logInfoStub};
        handleIncomingMessageStub = sandbox.stub(KafkaThrottlingManager.prototype, 'handleIncomingMessage');
        sandbox.stub(KafkaThrottlingManager.prototype, 'init');
        sandbox.stub(kafka, 'Offset');
        consumerGroupStreamStub = sandbox.stub(kafka, 'ConsumerGroupStream');
    });

    beforeEach(async function () {
        consumerEventHandlers = {};
        consumerStreamStub = {
            on: function (name, func) {
                consumerEventHandlers[name] = func;
            },
            close: () => {
            },
            consumerGroup: {},
            pause: () => {
            }
        };

        consumerGroupStreamStub.returns(consumerStreamStub);

        consumer = new KafkaStreamConsumer();

        promiseActionSpy = sinon.spy();

        baseConfiguration = {
            KafkaUrl: 'KafkaUrl',
            GroupId: 'GroupId',
            flowManagerInterval: 5555,
            throttlingThreshold: 555,
            Topics: ['topic-a', 'topic-b'],
            MessageFunction: promiseActionSpy,
            FetchMaxBytes: 9999
        };

        setTimeout(() => {
            consumerEventHandlers.connect();
        }, 100);

    });

    after(function () {
        sandbox.restore();
    });

    afterEach(function () {
        sandbox.reset();
    });

    it('Testing prometheus initializiation - shouldExposeMetrics=true', async function () {
        baseConfiguration.shouldExposeMetrics = true;
        await consumer.init(baseConfiguration, logger);
        consumerGroupStreamStub.returns(consumerStreamStub);
        should(consumer.kafkaQueryHistogram.name).equal('kafka_request_duration_seconds_bucket');
        should(consumer.kafkaQueryHistogram.help).equal('The duration time of processing kafka specific message');
        should(consumer.kafkaQueryHistogram.upperBounds).deepEqual(prometheusConfig.BUCKETS.PROMETHEUS_KAFKA_DURATION_SIZES_BUCKETS);
        should(consumer.kafkaConsumerGroupOffset.name).equal('kafka_consumer_group_offset_diff');
        should(consumer.kafkaConsumerGroupOffset.help).equal('The service\'s consumer groups offset');
        should(consumer.kafkaConsumerGroupOffset.labelNames).deepEqual(['topic', 'consumer_group', 'partition']);
    });
    it('Testing prometheus initializiation - shouldExposeMetrics=false (unspecified)', async function () {
        await consumer.init(baseConfiguration, logger);
        consumerGroupStreamStub.returns(consumerStreamStub);
        should(consumer.kafkaQueryHistogram).equal(undefined);
        should(consumer.kafkaConsumerGroupOffset).equal(undefined);
    });
});