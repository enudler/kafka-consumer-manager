'use strict';

let logger = require('../src/logger'),
    sinon = require('sinon'),
    should = require('should'),
    kafka = require('kafka-node'),
    rewire = require('rewire');

let sandbox,
    HighLevelProducerStub, clientStub,
    logTraceStub, configuration,
    logErrorStub, producerStub,
    producer, logInfoStub, producerSendStub,
    sendStub, onStub, producerEventHandlers;

describe('Testing kafka producer component', () =>{
    before(() => {
        sandbox = sinon.sandbox.create();
        logErrorStub = sandbox.stub(logger, 'error');
        logInfoStub = sandbox.stub(logger, 'info');
        logTraceStub = sandbox.stub(logger, 'trace');
        sendStub = sandbox.stub();
        producerSendStub = sandbox.stub();
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
    after(() => {
        sandbox.restore();
    });
    describe('Testing init method', function () {
        describe('When ready resolve', function () {
            afterEach(function () {
                sandbox.resetHistory();
            });

            it('Validation HighLevelProducer args', function () {
                producer = rewire('../src/kafkaProducer');

                configuration = {
                    KafkaUrl: 'kafka',
                    KafkaConnectionTimeout: 1000
                };

                setTimeout(() => { producerEventHandlers.ready('connect test') }, 100);
                return producer.init(configuration)
                    .then(() => {
                        should(logInfoStub.args[0][0]).eql('Producer is ready');
                    });
            });
        });

        describe('When ready is not resolved', function () {
            it('Should reject with error', function () {
                producer = rewire('../src/kafkaProducer');

                configuration = {
                    KafkaUrl: 'kafka',
                    KafkaConnectionTimeout: 100
                };

                return producer.init(configuration)
                    .then(function () {
                        throw new Error('should not get here.');
                    })
                    .catch(function (err) {
                        should(err.message).eql('Failed to connect to kafka after 100 ms.');
                    });
            });
        });
    });
    describe('Testing send method', function () {
        beforeEach(function () {
            producer = rewire('../src/kafkaProducer');

            configuration = {
                KafkaUrl: 'kafka',
                KafkaConnectionTimeout: 1000
            };

            producer.init(configuration);
        });
        describe('When send returns no error', function () {
            afterEach(function () {
                sandbox.resetHistory();
            });

            it('Should call producer.send function on send call', function (done) {
                sendStub.returns();

                producer.send('{}', 'some_topic');

                setTimeout(function () {
                    should(logTraceStub.args[0]).eql(['Producing message, to topic some_topic']);
                    var expectedResult = [{
                        topic: 'some_topic',
                        messages: ['{}']
                    }];
                    should(producerSendStub.args[0][0]).eql(expectedResult);
                    producerSendStub.args[0][1]();

                    done();
                }, 500);
            });
        });

        describe('When send returns error', function () {
            afterEach(function () {
                sandbox.resetHistory();
            });

            it('Should call producer.send function on send call', function (done) {
                sendStub.rejects();

                producer.send(JSON.stringify({key: 'value'}), 'some_topic');

                setTimeout(function () {
                    should(logTraceStub.args[0]).eql(['Producing message, to topic some_topic']);
                    let error = new Error('error');
                    producerSendStub.args[0][1](error);
                    should(logErrorStub.args[0][0]).eql('Failed to write message to Kafka: {"key":"value"}');
                    done();
                }, 500);
            });
        });
    });
});
