'use strict';

let kafka = require('kafka-node'),
    sinon = require('sinon'),
    rewire = require('rewire'),
    _ = require('lodash'),
    logger = require('../src/helpers/logger'),
    should = require('should');

let sandbox, offsetChecker,
    expectedError,
    consumerEventHandlers,
    logErrorStub, consumerStub, fetchStub,
    offsetStub,
    closeStub, pauseStub, resumeStub;

describe('Testing consumer offset out of sync checker', function () {
    before(function () {
        sandbox = sinon.sandbox.create();
        logErrorStub = sandbox.stub(logger, 'error');
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
        fetchStub = sandbox.stub();

        offsetStub = {
            fetch: fetchStub
        };
        offsetStub = sandbox.stub(kafka, 'Offset').returns(offsetStub);

        offsetChecker = rewire('../src/healthCheckers/consumerOffsetOutOfSyncChecker');

        let configuration = {
            KafkaOffsetDiffThreshold: 3
        };

        offsetChecker.init(consumerStub, configuration);
    });

    after(function () {
        sandbox.restore();
    });
    describe('Testing health check method', function () {
        beforeEach(function () {
            sandbox.reset();
        });

        it('Should resolve when no partitions data', function () {
            offsetChecker.__set__('previousConsumerReadOffset', []);

            consumerStub.topicPayloads = [];

            return offsetChecker.validateOffsetsAreSynced()
                .then(() => {
                    fetchStub.called.should.be.eql(false);
                });
        });

        it('Should resolve when all the partitions incremented from last check', function () {
            offsetChecker.__set__('previousConsumerReadOffset', [{topic: 'A', partition: 'B', offset: 1}]);

            consumerStub.topicPayloads = [{topic: 'A', partition: 'B', offset: 2}];

            return offsetChecker.validateOffsetsAreSynced()
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

            offsetChecker.__set__('previousConsumerReadOffset', [{topic: 'topic', partition: 'partition', offset: 1}]);

            consumerStub.topicPayloads = [{topic: 'topic', partition: 'partition', offset: 1}];

            return offsetChecker.validateOffsetsAreSynced()
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

            offsetChecker.__set__('previousConsumerReadOffset', [{topic: 'topicC', partition: 'partitionC', offset: 1}]);

            consumerStub.topicPayloads = [{topic: 'topicA', partition: 'partitionA', offset: 1}];

            return offsetChecker.validateOffsetsAreSynced()
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

            offsetChecker.__set__('previousConsumerReadOffset', [{
                topic: 'topicA',
                partition: 'partitionA',
                offset: 50
            }, {topic: 'topicB', partition: 'partitionB', offset: 100}]);

            consumerStub.topicPayloads = [{topic: 'topicA', partition: 'partitionA', offset: 60}, {
                topic: 'topicB',
                partition: 'partitionB',
                offset: 100
            }];

            return offsetChecker.validateOffsetsAreSynced()
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

            offsetChecker.__set__('previousConsumerReadOffset', [{topic: 'topicA', partition: 'partitionA', offset: 60},
                {topic: 'topicB', partition: 'partitionB', offset: 100}]);

            consumerStub.topicPayloads = [{topic: 'topicA', partition: 'partitionA', offset: 61},
                {topic: 'topicB', partition: 'partitionB', offset: 100}];

            offsetChecker.validateOffsetsAreSynced()
                .then(() => {
                    fetchStub.called.should.eql(true);
                    fetchStub.args[0][0].should.eql(expectedFetchArgs);
                    done();
                });
        });

        it('Should return an error when offset.fetch fails', function (done) {
            expectedError = new Error('error');
            fetchStub.yields(expectedError);

            offsetChecker.__set__('previousConsumerReadOffset', [{
                topic: 'topicA',
                partition: 'partitionA',
                offset: 60
            }]);

            consumerStub.topicPayloads = [{topic: 'topicA', partition: 'partitionA', offset: 60}];

            expectedError = new Error('error');
            fetchStub.yields(expectedError);
            offsetChecker.validateOffsetsAreSynced()
                .then(function () {
                    done(new Error('validateOffsetsAreSynced function did not throw an error as expected'));
                })
                .catch((error) => {
                    logErrorStub.args[0].should.eql([expectedError, 'Monitor Offset: Failed to fetch offsets']);
                    error.should.eql(new Error('Monitor Offset: Failed to fetch offsets:' + expectedError.message));
                    fetchStub.called.should.eql(true);
                    done();
                });
        });

        it('Should return an error when the consumer is NOT in sync', function (done) {
            fetchStub.yields(undefined, {
                'topicA': {
                    'partitionA': [60]
                },
                'topicB': {
                    'partitionB': [104]
                }
            });

            offsetChecker.__set__('previousConsumerReadOffset', [{topic: 'topicA', partition: 'partitionA', offset: 60},
                {topic: 'topicB', partition: 'partitionB', offset: 100}]);

            consumerStub.topicPayloads = [{topic: 'topicA', partition: 'partitionA', offset: 61},
                {topic: 'topicB', partition: 'partitionB', offset: 100}];

            offsetChecker.validateOffsetsAreSynced()
                .then(() => done(new Error('validateOffsetsAreSynced function did not throw an error as expected')))
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
            offsetChecker.__set__('previousConsumerReadOffset', [{topic: 'topicA', partition: 'partitionA', offset: 60}]);
            consumerStub.topicPayloads = [{topic: 'topicA', partition: 'partitionA', offset: 60}];
            fetchStub.yields(undefined, {
                'topicA': {}
            });
            return offsetChecker.validateOffsetsAreSynced()
                .then(() => Promise.reject(new Error('validateOffsetsAreSynced function did not throw an error as expected')))
                .catch((error) => {
                    fetchStub.called.should.eql(true);
                    // logInfoStub.args[0][0].should.eql('Monitor Offset: No progress detected in offsets since the last check. Checking that the consumer is in sync..');
                    logErrorStub.args[0][0].should.eql('Monitor Offset: Kafka consumer topics/partitions found to be out of sync in topic: topicA and in partition:partitionA');
                    error.should.eql(new Error('Monitor Offset: Kafka consumer topics/partitions found to be out of sync in topic: topicA and in partition:partitionA'));
                    fetchStub.called.should.eql(true);
                });
        });
    });
});
