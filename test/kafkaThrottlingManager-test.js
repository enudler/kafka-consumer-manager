let sinon = require('sinon'),
    should = require('should'),
    KafkaThrottlingManager = require('../src/throttling/kafkaThrottlingManager');

const sleep = require('util').promisify(setTimeout);

let sandbox, kafkaThrottlingManager, commitFunctionStub, logInfoStub, logTraceStub,
    consumerSetThirstyStub, consumerResumeStub, consumerPauseStub,
    kafkaStreamConsumer, commitStub, logger, logErrorStub;

describe('Testing kafkaThrottlingManager component', () => {
    before(() => {
        sandbox = sinon.sandbox.create();
        consumerSetThirstyStub = sandbox.stub();
        consumerResumeStub = sandbox.stub();
        consumerPauseStub = sandbox.stub();
        commitStub = sandbox.stub();

        kafkaStreamConsumer = {
            setThirsty: consumerSetThirstyStub,
            resume: consumerResumeStub,
            pause: consumerPauseStub,
            commit: commitStub
        };
    });

    // this describe represent one flow
    describe('Testing init and the manageQueue by interval', () => {
        let interval = 1000;
        let thresholdMessages = 20;
        let callbackFunc = new Promise((resolve, reject) => {
            setTimeout(() => {
                return resolve();
            }, 100);
        });
        let callbackErrorFunc = new Promise((resolve, reject) => {
            setTimeout(() => {
                return resolve();
            }, 100);
        });

        before(() => {
            logInfoStub = sandbox.stub();
            logTraceStub = sandbox.stub();
            logger = {error: sandbox.stub(), trace: logTraceStub, info: logInfoStub};
        });

        after(() => {
            kafkaThrottlingManager.stop();
            sandbox.reset();
            sandbox.restore();
        });

        it('Successful init to inner async queues', () => {
            kafkaThrottlingManager = new KafkaThrottlingManager();
            kafkaThrottlingManager.init(thresholdMessages,
                interval, ['TopicA', 'TopicB'], callbackFunc, callbackErrorFunc, kafkaStreamConsumer, logger);
            let queues = kafkaThrottlingManager.innerQueues;
            queues.should.eql({TopicA: {}, TopicB: {}});
        });

        it('Wait for first interval', (done) => {
            setTimeout(() => done(), interval);
        });

        it('Verify manageQueues was called and not paused', () => {
            should(logTraceStub.args[0][0]).eql('managing queues..');
            should(logTraceStub.args[1][0]).eql('Total messages in queues are: 0');
            should(consumerSetThirstyStub.args[0][0]).eql(true);
            should(consumerResumeStub.calledOnce).eql(true);
            should(consumerPauseStub.callCount).eql(0);
            sandbox.reset();
        });

        it('set number of messages to above the threshold', () => {
            for (let i = 0; i < thresholdMessages + 5; i++) {
                kafkaThrottlingManager.handleIncomingMessage({partition: 0, topic: 'TopicA', offset: i});
            }
        });

        it('Wait for second interval', (done) => {
            setTimeout(() => done(), interval);
        });

        it('Verify manageQueues was called and paused', () => {
            should(logTraceStub.args[0][0]).eql('managing queues..');
            should(logTraceStub.args[1][0]).eql('Total messages in queues are: 24');
            should(consumerSetThirstyStub.args[0][0]).eql(false);
            should(consumerResumeStub.callCount).eql(0);
            should(consumerPauseStub.calledOnce).eql(true);
        });
    });

    describe('handleIncomingMessage without metrics method tests', () => {
        before(async () => {
            logInfoStub = sandbox.stub();
            logTraceStub = sandbox.stub();
            logger = {error: sandbox.stub(), trace: logTraceStub, info: logInfoStub};

            commitFunctionStub = sandbox.stub();
            kafkaStreamConsumer.commit = commitFunctionStub;
            kafkaThrottlingManager = new KafkaThrottlingManager();
            kafkaThrottlingManager.init(1, 5000, ['TopicA', 'TopicB'], (msg) => {
                console.log('Invoking callback fn' + msg.msg);
                return Promise.resolve();
            }, () => Promise.resolve(), kafkaStreamConsumer, logger);
        });

        afterEach(() => {
            sandbox.reset();
        });

        after(() => {
            kafkaThrottlingManager.stop();
            sandbox.restore();
        });

        it('First call to handleIncomingMessage should create the right partition-queue ', async () => {
            let message = {
                topic: 'TopicA',
                partition: 4,
                msg: 'some-message1',
                offset: 1000
            };
            kafkaThrottlingManager.handleIncomingMessage(message);
            await sleep(1000);
            should(commitFunctionStub.calledOnce).eql(true);
            should(logTraceStub.args[0][0]).equal(`kafkaThrottlingManager finished handling message: topic: ${message.topic}, partition: ${message.partition}, offset: ${message.offset}`);
        });

        it('Second call to handleIncomingMessage should write to inner queue ', async () => {
            sandbox.resetHistory();

            let message = {
                topic: 'TopicA',
                partition: 4,
                msg: 'some-message2',
                offset: 1002
            };
            kafkaThrottlingManager.handleIncomingMessage(message);
            await sleep(100);
            should(commitFunctionStub.calledOnce).eql(true);
            should(logTraceStub.args[0][0]).equal(`kafkaThrottlingManager finished handling message: topic: ${message.topic}, partition: ${message.partition}, offset: ${message.offset}`);
        });
    });

    describe('handleIncomingMessage is failing', () => {
        let errorCallbackStub;
        before(async () => {
            logInfoStub = sandbox.stub();
            logTraceStub = sandbox.stub();
            logErrorStub = sandbox.stub();
            errorCallbackStub = sandbox.stub();
            logger = {error: logErrorStub, trace: logTraceStub, info: logInfoStub};

            commitFunctionStub = sandbox.stub();
            kafkaStreamConsumer.commit = commitFunctionStub;
            kafkaThrottlingManager = new KafkaThrottlingManager();
            kafkaThrottlingManager.init(1, 5000, ['TopicA', 'TopicB'],
                () => Promise.reject(new Error('some error message')),
                (msg, err) => {
                    return errorCallbackStub(msg, err);
                },
                kafkaStreamConsumer, logger);
        });

        afterEach(() => {
            sandbox.reset();
        });

        after(() => {
            kafkaThrottlingManager.stop();
            sandbox.restore();
        });

        it('handleIncomingMessage should rejected and write it to log ', async () => {
            sandbox.resetHistory();
            errorCallbackStub.yields('error function that resolves');
            let message = {
                topic: 'TopicA',
                partition: 4,
                msg: 'some-message',
                offset: 1005
            };
            let endStub = sandbox.stub();
            let histogram = {
                startTimer: sandbox.stub()
            };
            histogram.startTimer.returns(endStub);

            kafkaThrottlingManager.handleIncomingMessage(message, histogram);
            await sleep(100);
            should(commitFunctionStub.calledOnce).eql(true);
            should(errorCallbackStub.calledOnce).eql(true);
            should(errorCallbackStub.args[0][0]).deepEqual(message);
            should(errorCallbackStub.args[0][1]).deepEqual(new Error('some error message'));
            should(logErrorStub.args[0]).eql(['MessageFunction was rejected', new Error('some error message')]);
        });
        it('handleIncomingMessage should rejected and write it to log ', async () => {
            sandbox.resetHistory();
            errorCallbackStub.rejects('error function that rejects');
            let message = {
                topic: 'TopicA',
                partition: 4,
                msg: 'some-message',
                offset: 1005
            };
            let endStub = sandbox.stub();
            let histogram = {
                startTimer: sandbox.stub()
            };
            histogram.startTimer.returns(endStub);

            kafkaThrottlingManager.handleIncomingMessage(message, histogram);
            await sleep(100);
            should(commitFunctionStub.calledOnce).eql(true);
            should(errorCallbackStub.calledOnce).eql(true);
            should(errorCallbackStub.args[0][0]).deepEqual(message);
            should(logErrorStub.args[0]).eql(['MessageFunction was rejected', new Error('some error message')]);
            should(logErrorStub.args[1][0]).eql('ErrorMessageFunction invocation was rejected');
        });
    });
});