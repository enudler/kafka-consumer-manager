'use strict';

let sinon = require('sinon'),
    should = require('should'),
    kafka = require('kafka-node'),
    KafkaProducer = require('../src/producers/kafkaProducer'),
    assert = require('assert');

let sandbox,
    HighLevelProducerStub, clientStub,
    logTraceStub, configuration,
    logErrorStub, producerStub,
    producer, logInfoStub, producerSendStub,
    onStub, producerEventHandlers, logger;

describe('Testing kafka producer component', () => {
    before(() => {
        sandbox = sinon.sandbox.create();
        logErrorStub = sandbox.stub();
        logInfoStub = sandbox.stub();
        logTraceStub = sandbox.stub();
        producerSendStub = sandbox.stub();
        logger = {error: logErrorStub, trace: logTraceStub, info: logInfoStub};

        onStub = sandbox.stub();
        producerEventHandlers = {};

        producerStub = {
            on: function (name, func) {
                producerEventHandlers[name] = func;
            },
            send: producerSendStub
        };

        clientStub = {'client': 'object'};

        sandbox.stub(kafka, 'KafkaClient').returns(clientStub);
        HighLevelProducerStub = sandbox.stub(kafka, 'HighLevelProducer').returns(producerStub);
    });

    beforeEach(async () => {
        producer = new KafkaProducer();
    });
    after(() => {
        sandbox.restore();
    });
    describe('Testing init method', function () {
        describe('When ready resolve', function () {
            afterEach(function () {
                sandbox.resetHistory();
            });

            it('Validation HighLevelProducer args', function () {
                configuration = {
                    KafkaUrl: 'kafka',
                    KafkaConnectionTimeout: 1000,
                    WriteBackDelay: 100
                };

                setTimeout(() => {
                    producerEventHandlers.ready();
                }, 100);

                return producer.init(configuration, logger)
                    .then(() => {
                        should(logInfoStub.args[0][0]).eql('Producer is ready');
                    });
            });
        });

        describe('When ready is not resolved', function () { // todo - dont need it because there is connection timeout
            it('Should reject with error', function () {
                producer = new KafkaProducer();

                configuration = {
                    KafkaUrl: 'kafka',
                    KafkaConnectionTimeout: 100
                };

                return producer.init(configuration, logger)
                    .then(function () {
                        throw new Error('should not get here.');
                    })
                    .catch(function (err) {
                        should(err.message).eql('Failed to connect to kafka producer after 100 ms.');
                    });
            });
        });
    });
    describe('Testing send method', function () {
        beforeEach(function () {
            setTimeout(() => {
                producerEventHandlers.ready('connect test');
            }, 100);
        });
        describe('When send returns no error', function () {
            afterEach(function () {
                sandbox.resetHistory();
            });

            it('Should call producer.send function on send call and delay 100 ms when WriteBackDelay param specified', async function () {
                configuration = {
                    KafkaUrl: 'kafka',
                    KafkaConnectionTimeout: 500,
                    WriteBackDelay: 100
                };

                setTimeout(() => {
                    producerEventHandlers.ready('connect test');
                }, 25);

                await producer.init(configuration, logger);

                producerSendStub.yields(undefined, {});

                let startDate = Date.now();
                await producer.send('{}', 'some_topic');
                should(Date.now() - startDate).be.within(100, 150);
                should(logTraceStub.args[0]).eql(['Producing message, to topic some_topic']);
                var expectedResult = [{
                    topic: 'some_topic',
                    messages: ['{}']
                }];
                should(producerSendStub.args[0][0]).eql(expectedResult);
            });

            it('Should call producer.send function on send call without delay when WriteBackDelay not specified', async function () {
                configuration = {
                    KafkaUrl: 'kafka',
                    KafkaConnectionTimeout: 500
                };

                setTimeout(() => {
                    producerEventHandlers.ready('connect test');
                }, 3);

                await producer.init(configuration, logger);

                producerSendStub.yields(undefined, {});

                let startDate = Date.now();
                await producer.send('{}', 'some_topic');
                should(Date.now() - startDate).be.within(0, 10);
                should(logTraceStub.args[0]).eql(['Producing message, to topic some_topic']);
                var expectedResult = [{
                    topic: 'some_topic',
                    messages: ['{}']
                }];
                should(producerSendStub.args[0][0]).eql(expectedResult);
            });
        });

        describe('When send returns error', function () {
            afterEach(function () {
                sandbox.resetHistory();
            });

            it('Should call producer.send function on send call', async function () {
                configuration = {
                    KafkaUrl: 'kafka',
                    KafkaConnectionTimeout: 1000
                };

                await producer.init(configuration, logger);

                producerSendStub.yields(new Error('Error'));

                try {
                    await producer.send(JSON.stringify({key: 'value'}), 'some_topic');
                    assert.fail('send should fail');
                } catch (err) {
                    should(err.message).eql('Error');
                    should(logTraceStub.args[0]).eql(['Producing message, to topic some_topic']);
                    should(logErrorStub.args[0][0]).eql('Failed to write message to Kafka: {"key":"value"}');
                    should(logErrorStub.args[0][1].message).eql('Error');
                };
            });
        });
    });
});
