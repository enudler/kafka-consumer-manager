'use strict';

let kafka = require('kafka-node'),
    sinon = require('sinon'),
    rewire = require('rewire'),
    _ = require('lodash'),
    logger = require('../src/logger'),
    should = require('should');

let sandbox, consumer,
    expectedError,
    consumerGroupStub,
    consumerEventHandlers,
    logErrorStub, consumerStub, fetchStub,
    offsetStub, logInfoStub,
    closeStub, pauseStub, resumeStub, actionSpy;

describe('Testing kafka consumer component', function () {
    before(function () {
        sandbox = sinon.sandbox.create();
        logErrorStub = sandbox.stub(logger, 'error');
        logInfoStub = sandbox.stub(logger, 'info');
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
            resume: resumeStub
        };

        consumerGroupStub = sandbox.stub(kafka, 'ConsumerGroup').returns(consumerStub);

        fetchStub = sandbox.stub();

        offsetStub = {
            fetch: fetchStub
        };
        offsetStub = sandbox.stub(kafka, 'Offset').returns(offsetStub);

        consumer = rewire('../src/kafkaConsumer');

        actionSpy = sinon.spy();
        let configuration = {
            KafkaUrl: 'KafkaUrl',
            GroupId: 'GroupId',
            KafkaConnectionTimeout: 1000,
            Topics: ['topic-a', 'topic-b'],
            KafkaOffsetDiffThreshold: 3,
            MessageFunction: actionSpy,
            MaxMessagesInMemory: 100,
            ResumeMaxMessagesRatio: 0.25
        };

        consumer.init(configuration);
    });

    after(function () {
        sandbox.restore();
    });
    describe('Testing init method', function () {
        afterEach(function () {
            sandbox.resetHistory();
        });

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
            consumerEventHandlers.connect('connect test');
            setTimeout(function () {
                should(logInfoStub.args[0]).eql(['Kafka client is ready']);
                done();
            }, 10);
        });

        it('Testing pause function handling', function (done) {
            consumer.pause();
            setTimeout(function () {
                should(consumer.__get__('consumerEnabled')).eql(false);
                should(logInfoStub.args[0]).eql(['Suspending Kafka consumption']);
                should(pauseStub.calledOnce).eql(true);

                done();
            }, 10);
        });

        it('Testing resume function handling', function (done) {
            consumer.__set__('consumerEnabled', false);
            consumer.__set__('shuttingDown', false);
            consumer.resume();
            setTimeout(function () {
                should(consumer.__get__('consumerEnabled')).eql(true);
                should(logInfoStub.args[0]).eql(['Resuming Kafka consumption']);
                should(resumeStub.calledOnce).eql(true);
                done();
            }, 10);
        });
    });

    describe('Testing closeConnection method', function () {
        afterEach(function () {
            sandbox.resetHistory();
        });
        it('should write to info log and close consumer cause there is no error', function () {
            consumer.closeConnection();
            should(closeStub.called).eql(true);
            should(logInfoStub.args[0][0]).eql('Consumer is closing connection');
        });
        it('should write to error log and close consumer', function () {
            closeStub.returns('err');
            consumer.closeConnection();
            should(closeStub.called).eql(true);
            should(logErrorStub.args[0][0]).eql('Error when trying to close connection with kafka');
        });
    });

    describe('Testing health check method', function () {
        before(function () {
            sandbox.reset();
            consumer.__set__('ready', true);
        });
        afterEach(function () {
            sandbox.reset();
        });

        it('Should resolve if consumer is disabled', function () {
            consumer.__set__('consumerEnabled', false);
            return consumer.healthCheck()
                .then(() => {
                    logInfoStub.args[0][0].should.eql('Monitor Offset: Skipping check as the consumer is paused');
                    consumer.__set__('consumerEnabled', true);
                });
        });

        it('Should resolve if consumer is not connected yet', function () {
            consumer.__set__('consumerEnabled', true);
            consumer.__set__('previousConsumerReadOffset', undefined);

            return consumer.healthCheck()
                .then(() => {
                    logInfoStub.args[0][0].should.eql('Monitor Offset: Skipping check as the consumer is not ready');
                    consumer.__set__('consumerEnabled', true);
                    consumer.__set__('previousConsumerReadOffset', []);
                });
        });

        it('Should resolve when no partitions data', function () {
            consumer.__set__('consumerEnabled', true);
            consumer.__set__('previousConsumerReadOffset', []);

            consumerStub.topicPayloads = [];

            return consumer.healthCheck()
                .then(() => {
                    fetchStub.called.should.be.eql(false);
                });
        });

        it('Should resolve when all the partitions incremented from last check', function () {
            consumer.__set__('consumerEnabled', true);
            consumer.__set__('previousConsumerReadOffset', [{topic: 'A', partition: 'B', offset: 1}]);

            consumerStub.topicPayloads = [{topic: 'A', partition: 'B', offset: 2}];

            return consumer.healthCheck()
                .then(() => {
                    fetchStub.called.should.be.eql(false);
                });
        });

        it('Should return normally when offset not incremneted but the consumer is in sync with ZooKeeper', function () {
            let expectedFetchArgs = [{
                topic: 'topic',
                partition: 'partition',
                time: -1
            }];

            fetchStub.yields(undefined, {
                'topic': {
                    'partition': [1]
                }
            });

            consumer.__set__('consumerEnabled', true);
            consumer.__set__('previousConsumerReadOffset', [{topic: 'topic', partition: 'partition', offset: 1}]);

            consumerStub.topicPayloads = [{topic: 'topic', partition: 'partition', offset: 1}];

            return consumer.healthCheck()
                .then(() => {
                    fetchStub.called.should.eql(true);
                    fetchStub.args[0][0].should.eql(expectedFetchArgs);
                });
        });

        it('Should return normally when the partition is not available in in previousConsumerReadOffset', function () {
            fetchStub.yields(undefined, {
                'topicA': {
                    'partitionA': [1]
                }
            });

            consumer.__set__('consumerEnabled', true);
            consumer.__set__('previousConsumerReadOffset', [{topic: 'topicC', partition: 'partitionC', offset: 1}]);

            consumerStub.topicPayloads = [{topic: 'topicA', partition: 'partitionA', offset: 1}];

            return consumer.healthCheck()
                .then(() => {
                    fetchStub.called.should.eql(false);
                });
        });

        it('Should return normally when one of the offset not incremented but the consumer is in sync with ZooKeeper', function () {
            let expectedFetchArgs = [{
                topic: 'topicB',
                partition: 'partitionB',
                time: -1
            }];

            fetchStub.yields(undefined, {
                'topicA': {
                    'partitionA': [60]
                },
                'topicB': {
                    'partitionB': [100]
                }
            });

            consumer.__set__('consumerEnabled', true);
            consumer.__set__('previousConsumerReadOffset', [{
                topic: 'topicA',
                partition: 'partitionA',
                offset: 50
            }, {topic: 'topicB', partition: 'partitionB', offset: 100}]);

            consumerStub.topicPayloads = [{topic: 'topicA', partition: 'partitionA', offset: 60}, {
                topic: 'topicB',
                partition: 'partitionB',
                offset: 100
            }];

            return consumer.healthCheck()
                .then(() => {
                    fetchStub.called.should.eql(true);
                    fetchStub.args[0][0].should.eql(expectedFetchArgs);
                });
        });

        it('Should NOT return an error when the consumer is of of sync less than 3 messages', function (done) {
            let expectedFetchArgs = [{
                topic: 'topicB',
                partition: 'partitionB',
                time: -1
            }];

            fetchStub.yields(undefined, {
                'topicA': {
                    'partitionA': [60]
                },
                'topicB': {
                    'partitionB': [103]
                }
            });

            consumer.__set__('consumerEnabled', true);
            consumer.__set__('previousConsumerReadOffset', [{topic: 'topicA', partition: 'partitionA', offset: 60},
                {topic: 'topicB', partition: 'partitionB', offset: 100}]);

            consumerStub.topicPayloads = [{topic: 'topicA', partition: 'partitionA', offset: 61},
                {topic: 'topicB', partition: 'partitionB', offset: 100}];

            consumer.healthCheck()
                .then(() => {
                    fetchStub.called.should.eql(true);
                    fetchStub.args[0][0].should.eql(expectedFetchArgs);
                    done();
                });
        });

        it('Should return an error when offset.fetch fails', function (done) {
            expectedError = new Error('error');
            fetchStub.yields(expectedError);

            consumer.__set__('consumerEnabled', true);
            consumer.__set__('previousConsumerReadOffset', [{
                topic: 'topicA',
                partition: 'partitionA',
                offset: 60
            }]);

            consumerStub.topicPayloads = [{topic: 'topicA', partition: 'partitionA', offset: 60}];

            expectedError = new Error('error');
            fetchStub.yields(expectedError);
            consumer.healthCheck()
                .then(function () {
                    done(new Error('healthCheck function did not throw an error as expected'));
                })
                .catch((error) => {
                    logErrorStub.args[0].should.eql([expectedError, 'Monitor Offset: Failed to fetch offsets']);
                    error.should.eql(new Error('Monitor Offset: Failed to fetch offsets:' + expectedError.message));
                    fetchStub.called.should.eql(true);
                    done();
                });
        });

        it('Should return an error when the consumer is NOT in sync', function (done) {
            let expectedFetchArgs = [{
                topic: 'topicB',
                partition: 'partitionB',
                time: -1
            }];

            fetchStub.yields(undefined, {
                'topicA': {
                    'partitionA': [60]
                },
                'topicB': {
                    'partitionB': [104]
                }
            });

            consumer.__set__('consumerEnabled', true);
            consumer.__set__('previousConsumerReadOffset', [{topic: 'topicA', partition: 'partitionA', offset: 60},
                {topic: 'topicB', partition: 'partitionB', offset: 100}]);

            consumerStub.topicPayloads = [{topic: 'topicA', partition: 'partitionA', offset: 61},
                {topic: 'topicB', partition: 'partitionB', offset: 100}];

            consumer.healthCheck()
                .then(() => done(new Error('healthCheck function did not throw an error as expected')))
                .catch((error) => {
                    let state = {
                        topic: 'topicB',
                        partition: 'partitionB',
                        partitionLatestOffset: 104,
                        partitionReadOffset: 100,
                        unhandledMessages: 4
                    };

                    fetchStub.called.should.eql(true);
                    logErrorStub.args[0].should.eql(['Monitor Offset: Kafka consumer offsets found to be out of sync', state]);
                    error.should.eql(new Error(('Monitor Offset: Kafka consumer offsets found to be out of sync:' + JSON.stringify(state))));
                    fetchStub.called.should.eql(true);
                    done();
                });
        });

        it('Should return an error when the consumer topics/partitions is NOT in sync', function () {
            consumer.__set__('consumerEnabled', true);
            consumer.__set__('previousConsumerReadOffset', [{topic: 'topicA', partition: 'partitionA', offset: 60}]);
            consumerStub.topicPayloads = [{topic: 'topicA', partition: 'partitionA', offset: 60}];
            fetchStub.yields(undefined, {
                'topicA': {}
            });
            return consumer.healthCheck()
                .then(() => Promise.reject(new Error('healthCheck function did not throw an error as expected')))
                .catch((error) => {
                    fetchStub.called.should.eql(true);
                    // logInfoStub.args[0][0].should.eql('Monitor Offset: No progress detected in offsets since the last check. Checking that the consumer is in sync..');
                    logErrorStub.args[0][0].should.eql('Monitor Offset: Kafka consumer topics/partitions found to be out of sync in topic: topicA and in partition:partitionA');
                    error.should.eql(new Error('Monitor Offset: Kafka consumer topics/partitions found to be out of sync in topic: topicA and in partition:partitionA'));
                    fetchStub.called.should.eql(true);
                });
        });
    });

    describe('Testing Max messages in memory', function () {
        before(function () {
            sandbox.reset();
            consumer.__set__('ready', true);
            consumer.__set__('messagesInMemory', 0);
            consumer.__set__('consumerEnabled', true);
            consumer.__set__('shuttingDown', false);
        });

        beforeEach(function () {
            consumer.__set__('messagesInMemory', 0);
            consumer.__set__('consumerEnabled', true);
        });

        afterEach(function () {
            sandbox.reset();
        });

        it('Should pause when getting to max message (100)', function () {
            for (let i = 1; i <= 100; i++) {
                consumerEventHandlers.message({});
                consumer.__get__('consumerEnabled').should.be.eql(i !== 100);
            }

            logInfoStub.args[0][0].should.eql('Reached 100 messages (max is 100), pausing kafka consumers');

            for (let i = 0; i < 100; i++) {
                consumerEventHandlers.message({});
                consumer.__get__('consumerEnabled').should.be.eql(false);
            }
        });

        it('Should resume consuming when messages in memory is 25 (by configuration)', function () {
            for (let i = 1; i <= 200; i++) {
                consumerEventHandlers.message({});
            }

            logInfoStub.args[0][0].should.eql('Reached 100 messages (max is 100), pausing kafka consumers');

            for (let i = 175; i >= 0; i--) {
                consumer.decreaseMessageInMemory();
                consumer.__get__('consumerEnabled').should.be.eql(i === 0);
            }
        });
    });
});
