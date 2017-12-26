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
    offsetStub, logInfoStub, topicPayloads,
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

        topicPayloads = [
            {
                topic: 'topic-a',
                partition: 0,
                offset: 312
            }
        ];

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
            topicPayloads: topicPayloads
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
            KAFKA_URL: 'kafka_url',
            GROUP_ID: 'group_id',
            KAFKA_CONNECTION_TIMEOUT: 'kafka_connection_timeout',
            TOPICS: ['topic-a', 'topic-b'],
            KAFKA_OFFSET_DIFF_THRESHOLD: 3,
            MESSAGE_FUNCTION: actionSpy
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
                'groupId': 'group_id',
                'protocol': [
                    'roundrobin'
                ],
                'sessionTimeout': 10000,
                'kafkaHost': 'kafka_url'
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

        it('Should return normally when the consumer is in sync with ZooKeeper', function () {
            let expectedFetchArgs = _.map(topicPayloads, function (topicPayload) {
                return {
                    topic: topicPayload.topic,
                    partition: topicPayload.partition,
                    time: -1
                };
            });
            fetchStub.yields(undefined, {
                [topicPayloads[0].topic]: {
                    [topicPayloads[0].partition]: [topicPayloads[0].offset]
                }
            });
            return consumer.healthCheck()
                .then(() => {
                    // logInfoStub.args[0][0].should.eql('Monitor Offset: No progress detected in offsets since the last check. Checking that the consumer is in sync..');
                    // logInfoStub.args[1][0].should.eql('Monitor Offset: Consumer found to be in sync');
                    fetchStub.called.should.eql(true);
                    fetchStub.args[0][0].should.eql(expectedFetchArgs);
                });
        });

        it('Should NOT return an error when the consumer is of of sync less than 3 messages', function () {
            let expectedFetchArgs = _.map(topicPayloads, function (topicPayload) {
                return {
                    topic: topicPayload.topic,
                    partition: topicPayload.partition,
                    time: -1
                };
            });
            fetchStub.yields(undefined, {
                [topicPayloads[0].topic]: {
                    [topicPayloads[0].partition]: [topicPayloads[0].offset + 2]
                }
            });
            return consumer.healthCheck()
                .then(() => {
                    // logInfoStub.args[0][0].should.eql('Monitor Offset: No progress detected in offsets since the last check. Checking that the consumer is in sync..');
                    // logInfoStub.args[1][0].should.eql('Monitor Offset: Consumer found to be in sync');
                    fetchStub.called.should.eql(true);
                    fetchStub.args[0][0].should.eql(expectedFetchArgs);
                });
        });

        it('Should return an error when offset.fetch fails', function () {
            expectedError = new Error('error');
            fetchStub.yields(expectedError);
            return consumer.healthCheck()
                .then(function () {
                    Promise.reject(new Error('healthCheck function did not throw an error as expected'));
                })
                .catch((error) => {
                    // logInfoStub.args[0][0].should.eql('Monitor Offset: No progress detected in offsets since the last check. Checking that the consumer is in sync..');
                    logErrorStub.args[0].should.eql([expectedError, 'Monitor Offset: Failed to fetch offsets']);
                    error.should.eql(new Error('Monitor Offset: Failed to fetch offsets:' + expectedError.message));
                    fetchStub.called.should.eql(true);
                });
        });

        it('Should return an error when the consumer is NOT in sync', function () {
            fetchStub.yields(undefined, {
                [topicPayloads[0].topic]: {
                    [topicPayloads[0].partition]: [topicPayloads[0].offset + 3]
                }
            });
            return consumer.healthCheck()
                .then(() => Promise.reject(new Error('healthCheck function did not throw an error as expected')))
                .catch((error) => {
                    let state = {
                        topic: topicPayloads[0].topic,
                        partition: topicPayloads[0].partition,
                        partitionLatestOffset: 315,
                        partitionReadOffset: 312,
                        unhandledMessages: 3
                    };

                    fetchStub.called.should.eql(true);
                    // logInfoStub.args[0][0].should.eql('Monitor Offset: No progress detected in offsets since the last check. Checking that the consumer is in sync..');
                    logErrorStub.args[0].should.eql(['Monitor Offset: Kafka consumer offsets found to be out of sync', state]);
                    error.should.eql(new Error(('Monitor Offset: Kafka consumer offsets found to be out of sync:' + JSON.stringify(state))));
                    fetchStub.called.should.eql(true);
                });
        });

        it('Should return an error when the consumer topics/partitions is NOT in sync', function () {
            fetchStub.yields(undefined, {
                [topicPayloads[0].topic]: {}
            });
            return consumer.healthCheck()
                .then(() => Promise.reject(new Error('healthCheck function did not throw an error as expected')))
                .catch((error) => {
                    fetchStub.called.should.eql(true);
                    // logInfoStub.args[0][0].should.eql('Monitor Offset: No progress detected in offsets since the last check. Checking that the consumer is in sync..');
                    logErrorStub.args[0][0].should.eql('Monitor Offset: Kafka consumer topics/partitions found to be out of sync in topic: topic-a and in partition:0');
                    error.should.eql(new Error('Monitor Offset: Kafka consumer topics/partitions found to be out of sync in topic: topic-a and in partition:0'));
                    fetchStub.called.should.eql(true);
                });
        });
    });
});
