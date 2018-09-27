'use strict';

let kafka = require('kafka-node'),
    sinon = require('sinon'),
    _ = require('lodash'),
    should = require('should'),
    KafkaConsumer = require('../src/consumers/kafkaConsumer'),
    assert = require('assert');

let sandbox, consumer,
    consumerGroupStub,
    consumerEventHandlers,
    logErrorStub, consumerStub, fetchStub,
    offsetStub, logInfoStub,
    closeStub, pauseStub, resumeStub, actionSpy,
    validateOffsetsAreSyncedStub,logger;

describe('Testing kafka consumer component', function () {
    before(function () {
        sandbox = sinon.sandbox.create();
        logErrorStub = sandbox.stub();
        logInfoStub = sandbox.stub();
        logger = {error: logErrorStub, trace: sandbox.stub(), info: logInfoStub};

        closeStub = sandbox.stub();
        pauseStub = sandbox.stub();
        resumeStub = sandbox.stub();
        consumerEventHandlers = {};

        consumerStub = {
            on: function (name, func) {
                consumerEventHandlers[name] = func;
            },
            close: function (cb) {
                let err = closeStub();
                cb(err);
            },
            pause: pauseStub,
            resume: resumeStub,
            topicPayloads: [{
                'topic': 'events_topic',
                'partition': 0,
                'offset': 88,
                'maxBytes': 1048576,
                'metadata': 'm'
            }]
        };

        consumerGroupStub = sandbox.stub(kafka, 'ConsumerGroup');

        fetchStub = sandbox.stub();

        offsetStub = {
            fetch: fetchStub
        };
        offsetStub = sandbox.stub(kafka, 'Offset').returns(offsetStub);
    });

    beforeEach(async () => {
        consumerGroupStub.returns(consumerStub);

        actionSpy = sinon.spy();
        let configuration = {
            KafkaUrl: 'KafkaUrl',
            GroupId: 'GroupId',
            KafkaConnectionTimeout: 2000,
            Topics: ['topic-a', 'topic-b'],
            KafkaOffsetDiffThreshold: 3,
            MessageFunction: actionSpy,
            MaxMessagesInMemory: 100,
            ResumeMaxMessagesRatio: 0.25
        };

        consumer = new KafkaConsumer();

        setTimeout(() => {
            consumerEventHandlers.connect();
        }, 100);
        await consumer.init(configuration, logger);
    });

    afterEach(function () {
        sandbox.reset();
    });

    after(() => {
        sandbox.restore();
    });

    describe('Testing init method', function () {
        it('Validation consumerGroup Options,topic', function () {
            consumerGroupStub.returns(consumerStub);

            let optionsExpected = {
                'autoCommit': true,
                'encoding': 'utf8',
                'groupId': 'GroupId',
                'protocol': [
                    'roundrobin'
                ],
                'sessionTimeout': 10000,
                'kafkaHost': 'KafkaUrl',
                'fetchMaxBytes': 1048576
            };

            should(consumerGroupStub.args[0][1]).eql(['topic-a', 'topic-b']);
            should(_.pickBy(consumerGroupStub.args[0][0])).eql(optionsExpected);
        });

        it('Testing consumer message event handling simulate valid message', function (done) {
            // call the the event
            consumerGroupStub.returns(consumerStub);

            let msg = {
                value: 'some_value',
                partition: 123,
                offest: 5
            };

            consumerEventHandlers.message(msg);
            setTimeout(function () {
                should(consumer.getLastMessage()).deepEqual(msg);
                should(actionSpy.calledOnce).eql(true);
                should(actionSpy.args.length).eql(1);
                should(actionSpy.args[0][0]).eql(msg);
                done();
            }, 10);
        });

        it('Testing consumer error event', function (done) {
            consumerEventHandlers.error(new Error('error test'));
            setTimeout(function () {
                should(logErrorStub.args[0]).eql([new Error('error test'), 'Kafka Error']);
                done();
            }, 10);
        });

        it('Testing consumer offsetOutOfRange event', function (done) {
            consumerEventHandlers.offsetOutOfRange('offsetOutOfRange test');
            setTimeout(function () {
                should(logErrorStub.args[0]).eql(['offsetOutOfRange test', 'offsetOutOfRange Error']);
                done();
            }, 10);
        });

        it('Testing consumer connect event', function (done) {
            consumerStub.topicPayloads = [{}];
            consumerEventHandlers.connect();
            setTimeout(function () {
                should(logInfoStub.args[0]).eql(['Kafka client is ready']);
                done();
            }, 10);
        });

        it('Testing pause function handling', function () {
            consumer.pause();
            should(consumer.consumerEnabled).eql(false);
            should(logInfoStub.args[2]).eql(['Suspending Kafka consumption']);
            should(pauseStub.calledOnce).eql(true);
        });

        it('Testing resume function handling - too many messages in memory', function () {
            consumer.setThirsty(false);
            consumer.setDependencyHealthy(true);
            consumer.resume();
            should(logInfoStub.args[2][0]).eql('Not resuming consumption because too many messages in memory');
            should(resumeStub.calledOnce).eql(false);
        });

        it('Testing resume function handling - dependency not healthy', function () {
            consumer.setThirsty(true);
            consumer.setDependencyHealthy(false);
            consumer.resume();
            should(logInfoStub.args[2][0]).eql('Not resuming consumption because dependency check returned false');
            should(resumeStub.calledOnce).eql(false);
        });

        it('Testing resume function handling', function () {
            consumer.setThirsty(true);
            consumer.setDependencyHealthy(true);
            consumer.consumerEnabled = false;
            consumer.shuttingDown = false;
            consumer.resume();
            should(consumer.consumerEnabled).eql(true);
            should(logInfoStub.args[2]).eql(['Resuming Kafka consumption']);
            should(resumeStub.calledOnce).eql(true);
        });
    });

    describe('Testing closeConnection method', function () {
        it('should write to info log and close consumer cause there is no error', async function () {
            consumer.close = (cb) => { cb() };
            await consumer.closeConnection();
            should(closeStub.called).eql(true);
            should(logInfoStub.args[2][0]).eql('Consumer is closing connection');
        });
        it('should write to error log and close consumer', async function () {
            let err = new Error('close was failing');
            consumer.close = (cb) => { cb(_.cloneDeep(err)) };
            closeStub.returns('err');
            try {
                await consumer.closeConnection();
                assert.fail('close connection need to fail');
            } catch (err){
                should(err).eql(err);
            }
            // should(closeStub.called).eql(true);
            //                should(logErrorStub.args[0][0]).eql('Error when trying to close connection with kafka');
        });
    });

    describe('Testing Max messages in memory', function () {
        beforeEach(function () {
            consumer.messagesInMemory = 0;
            consumer.consumerEnabled = true;
            consumer.shuttingDown = false;
        });

        it('Should pause when getting to max message (100)', function () {
            for (let i = 1; i <= 100; i++) {
                consumerEventHandlers.message({});
                (consumer.consumerEnabled).should.be.eql(i !== 100);
            }

            logInfoStub.args[2][0].should.eql('Reached 100 messages (max is 100), pausing kafka consumers');

            for (let i = 0; i < 100; i++) {
                consumerEventHandlers.message({});
                (consumer.consumerEnabled).should.be.eql(false);
            }
        });

        it('Should resume consuming when messages in memory is 25 (by configuration)', function () {
            for (let i = 1; i <= 200; i++) {
                consumerEventHandlers.message({});
            }

            logInfoStub.args[2][0].should.eql('Reached 100 messages (max is 100), pausing kafka consumers');

            for (let i = 175; i >= 0; i--) {
                consumer.decreaseMessageInMemory();
                (consumer.consumerEnabled).should.be.eql(i === 0);
            }
        });
    });

    describe('testing validateOffsetsAreSynced methods', function () {
        beforeEach(function () {
            consumer.messagesInMemory = 0;
            consumer.consumerEnabled = true;
            consumer.shuttingDown = false;

            validateOffsetsAreSyncedStub = sandbox.stub(consumer.consumerOffsetOutOfSyncChecker, 'validateOffsetsAreSynced');
            validateOffsetsAreSyncedStub.resolves();
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
});
