'use strict';

let kafka = require('kafka-node'),
    sinon = require('sinon'),
    kafkaThrottlingManager = require('../src/throttling/kafkaThrottlingManager'),
    consumerOffsetOutOfSyncChecker = require('../src/healthCheckers/consumerOffsetOutOfSyncChecker'),
    logger = require('../src/helpers/logger'),
    rewire = require('rewire'),
    should = require('should');

let sandbox, logErrorStub, logTraceStub, logInfoStub, consumerGroupStreamStub,
    consumerStreamStub, consumerEventHandlers, resumeStub, pauseStub, consumer,
    promiseActionSpy, baseConfiguration, handleIncomingMessageStub, commitStub, closeStub,
    kafkaThrottlingManagerInitStub, consumerOffsetOutOfSyncCheckerInitStub, validateOffsetsAreSyncedStub,
    kafkaStreamConsumer;

describe('Testing init method', function () {
    beforeEach(function () {
        sandbox = sinon.sandbox.create();
        logErrorStub = sandbox.stub(logger, 'error');
        logInfoStub = sandbox.stub(logger, 'info');
        logTraceStub = sandbox.stub(logger, 'trace');
        handleIncomingMessageStub = sandbox.stub(kafkaThrottlingManager, 'handleIncomingMessage');
        kafkaThrottlingManagerInitStub = sandbox.stub(kafkaThrottlingManager, 'init');
        consumerOffsetOutOfSyncCheckerInitStub = sandbox.stub(consumerOffsetOutOfSyncChecker, 'init');

        consumerEventHandlers = {};

        consumerStreamStub = {
            on: function (name, func) {
                consumerEventHandlers[name] = func;
            }
        };

        consumerGroupStreamStub = sandbox.stub(kafka, 'ConsumerGroupStream').returns(consumerStreamStub);

        consumer = rewire('../src/consumers/kafkaStreamConsumer');

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
    afterEach(function () {
        sandbox.restore();
    });

    it('testing right configuration was called, full configuration', function () {
        consumer.init(baseConfiguration);
        consumerGroupStreamStub.returns(consumerStreamStub);

        let optionsExpected = {
            'autoCommit': false,
            'encoding': 'utf8',
            'groupId': 'GroupId',
            'protocol': [
                'roundrobin'
            ],
            'sessionTimeout': 10000,
            'host': undefined,
            'kafkaHost': 'KafkaUrl',
            'fetchMaxBytes': 128
        };

        should(consumerGroupStreamStub.args[0][1]).eql(['topic-a', 'topic-b']);
        should(consumerGroupStreamStub.args[0][0]).eql(optionsExpected);
    });

    it('testing right configuration was called, only mandatory configuration', function () {
        let baseConfiguration = {
            KafkaUrl: 'KafkaUrl',
            GroupId: 'GroupId',
            ThrottlingThreshold: 1000,
            ThrottlingCheckIntervalMs: 10000,
            Topics: ['topic-a', 'topic-b'],
            MessageFunction: () => {
            }
        };
        consumer.init(baseConfiguration);
        consumerGroupStreamStub.returns(consumerStreamStub);

        let optionsExpected = {
            'autoCommit': false,
            'encoding': 'utf8',
            'groupId': 'GroupId',
            'host': undefined,
            'protocol': [
                'roundrobin'
            ],
            'sessionTimeout': 10000,
            'kafkaHost': 'KafkaUrl',
            'fetchMaxBytes': 1048576

        };

        should(consumerGroupStreamStub.args[0][1]).eql(['topic-a', 'topic-b']);
        should(consumerGroupStreamStub.args[0][0]).eql(optionsExpected);
    });
    it('testing listening functions - on data', function (done) {
        consumer.init(baseConfiguration);
        consumerGroupStreamStub.returns(consumerStreamStub);

        let msg = {
            value: 'some_value',
            partition: 123,
            offset: 5,
            topic: 'my_topic'
        };
        consumerEventHandlers.data(msg);
        setTimeout(function () {
            sinon.assert.calledOnce(logTraceStub);
            sinon.assert.calledWithExactly(logTraceStub, 'consumerGroupStream got message: topic: my_topic, partition: 123, offset: 5');
            sinon.assert.calledOnce(handleIncomingMessageStub);
            sinon.assert.calledWithExactly(handleIncomingMessageStub, msg);
            done();
        }, 10);
    });
    it('testing listening functions - on error', function (done) {
        consumer.init(baseConfiguration);
        consumerGroupStreamStub.returns(consumerStreamStub);

        let err = {
            message: 'some_error_message',
            stack: 'some_error_stack'
        };
        consumerEventHandlers.error(err);
        setTimeout(function () {
            sinon.assert.calledOnce(logErrorStub);
            sinon.assert.calledWithExactly(logErrorStub, err, 'Kafka Error');
            done();
        }, 10);
    });
    it('testing listening functions - on close', function (done) {
        consumer.init(baseConfiguration);
        consumerGroupStreamStub.returns(consumerStreamStub);

        consumerEventHandlers.close();
        setTimeout(function () {
            sinon.assert.calledOnce(logInfoStub);
            sinon.assert.calledWithExactly(logInfoStub, 'Inner ConsumerGroupStream closed');
            done();
        }, 10);
    });
});

describe('Testing commit, pause and resume  methods', function () {
    beforeEach(function () {
        sandbox = sinon.sandbox.create();
        logInfoStub = sandbox.stub(logger, 'info');
        handleIncomingMessageStub = sandbox.stub(kafkaThrottlingManager, 'handleIncomingMessage');
        kafkaThrottlingManagerInitStub = sandbox.stub(kafkaThrottlingManager, 'init');
        consumerOffsetOutOfSyncCheckerInitStub = sandbox.stub(consumerOffsetOutOfSyncChecker, 'init');
        pauseStub = sandbox.stub();
        resumeStub = sandbox.stub();
        commitStub = sandbox.stub();
        consumerEventHandlers = {};

        consumerStreamStub = {
            on: function (name, func) {
                consumerEventHandlers[name] = func;
            },
            pause: pauseStub,
            resume: resumeStub,
            commit: commitStub
        };

        consumerGroupStreamStub = sandbox.stub(kafka, 'ConsumerGroupStream').returns(consumerStreamStub);

        consumer = rewire('../src/consumers/kafkaStreamConsumer');

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
        sandbox.restore();
    });

    it('testing resume function handling - too many messages in memory', function () {
        consumer.init(baseConfiguration);
        consumer.setThirsty(false);
        consumer.setDependencyHealthy(true);
        consumer.resume();
        should(logInfoStub.args[0][0]).eql('Not resuming consumption because too many messages in memory');
        should(resumeStub.calledOnce).eql(false);
    });

    it('Testing resume function handling - dependency not healthy', function () {
        consumer.setThirsty(true);
        consumer.setDependencyHealthy(false);
        consumer.resume();
        should(logInfoStub.args[0][0]).eql('Not resuming consumption because dependency check returned false');
        should(resumeStub.calledOnce).eql(false);
    });

    it('testing pause & resume methods', function () {
        consumer.init(baseConfiguration);
        consumerGroupStreamStub.returns(consumerStreamStub);
        consumer.pause();
        sinon.assert.calledOnce(logInfoStub);
        sinon.assert.calledWithExactly(logInfoStub, 'Suspending Kafka consumption');
        sinon.assert.calledOnce(pauseStub);
        consumer.resume();
        should(logInfoStub.args[1][0]).eql('Resuming Kafka consumption');
        should(logInfoStub.callCount).eql(2);
        sinon.assert.calledOnce(resumeStub);
    });

    it('testing commit methods', function () {
        let msg = {
            value: 'some_value',
            partition: 123,
            offset: 5,
            topic: 'my_topic'
        };
        consumer.init(baseConfiguration);
        consumerGroupStreamStub.returns(consumerStreamStub);
        consumer.commit(msg);
        sinon.assert.calledOnce(commitStub);
        sinon.assert.calledWithExactly(commitStub, msg, true);
    });
});

describe('testing validateOffsetsAreSynced methods', function () {
    beforeEach(function () {
        sandbox = sinon.sandbox.create();
        logInfoStub = sandbox.stub(logger, 'info');
        validateOffsetsAreSyncedStub = sandbox.stub(consumerOffsetOutOfSyncChecker, 'validateOffsetsAreSynced');
        validateOffsetsAreSyncedStub.resolves();

        consumer = rewire('../src/consumers/kafkaStreamConsumer');
    });
    afterEach(function () {
        sandbox.restore();
    });

    it('validateOffsetsAreSynced is not called since no consumer is not enabled yet', function () {
        consumer.__set__('consumerEnabled', false);
        return consumer.validateOffsetsAreSynced()
            .then(() => {
                should(logInfoStub.args[0][0]).eql('Monitor Offset: Skipping check as the consumer is paused');
                sinon.assert.callCount(validateOffsetsAreSyncedStub, 0);
            });
    });

    it('validateOffsetsAreSynced is called since consumer is enabled', function () {
        consumer.__set__('consumerEnabled', true);
        return consumer.validateOffsetsAreSynced()
            .then(() => {
                sinon.assert.callCount(validateOffsetsAreSyncedStub, 1);
            });
    });
});

describe('Testing closeConnection method', function () {
    beforeEach(function () {
        sandbox = sinon.sandbox.create();
        logInfoStub = sandbox.stub(logger, 'info');
        logErrorStub = sandbox.stub(logger, 'error');
        handleIncomingMessageStub = sandbox.stub(kafkaThrottlingManager, 'handleIncomingMessage');
        kafkaThrottlingManagerInitStub = sandbox.stub(kafkaThrottlingManager, 'init');
        consumerOffsetOutOfSyncCheckerInitStub = sandbox.stub(consumerOffsetOutOfSyncChecker, 'init');
        closeStub = sandbox.stub();
        consumerEventHandlers = {};
        consumerStreamStub = {
            on: function (name, func) {
                consumerEventHandlers[name] = func;
            },
            close: closeStub
        };

        consumerGroupStreamStub = sandbox.stub(kafka, 'ConsumerGroupStream').returns(consumerStreamStub);

        consumer = rewire('../src/consumers/kafkaStreamConsumer');

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
        sandbox.restore();
    });

    it('Testing closeConnection method - successful closure', function () {
        consumer.init(baseConfiguration);
        consumerGroupStreamStub.returns(consumerStreamStub);
        consumer.closeConnection();
        sinon.assert.calledOnce(closeStub);
        sinon.assert.calledOnce(logInfoStub);
        sinon.assert.calledWithExactly(logInfoStub, 'Consumer is closing connection');
    });
    it('Testing closeConnection method - failure in closure', function () {
        closeStub.yields({
            message: 'error message'
        });
        consumer.init(baseConfiguration);
        consumerGroupStreamStub.returns(consumerStreamStub);
        consumer.closeConnection();
        sinon.assert.calledOnce(closeStub);
        sinon.assert.calledOnce(logInfoStub);
        sinon.assert.calledWithExactly(logInfoStub, 'Consumer is closing connection');
        sinon.assert.calledOnce(logErrorStub);
        sinon.assert.calledWithExactly(logErrorStub, 'Error when trying to close connection with kafka', {
            errorMessage: 'error message'
        });
    });
});