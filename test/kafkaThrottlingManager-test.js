let sinon = require('sinon'),
    rewire = require('rewire'),
    async = require('async'),
    logger = require('../src/helpers/logger'),
    kafkaStreamConsumer = require('../src/consumers/kafkaStreamConsumer'),
    should = require('should');
let sandbox, kafkaThrottlingManager, commitFunctionStub, logInfoStub, logTraceStub, innerQueuePushStub,
    asyncQueueStub, consumerSetThirstyStub, consumerResumeStub, consumerPauseStub;

describe('Testing kafkaThrottlingManager component', () => {
    before(() => {
        sandbox = sinon.sandbox.create();
        logInfoStub = sandbox.stub(logger, 'info');
        logTraceStub = sandbox.stub(logger, 'trace');
        consumerSetThirstyStub = sandbox.stub(kafkaStreamConsumer, 'setThirsty');
        consumerResumeStub = sandbox.stub(kafkaStreamConsumer, 'resume');
        consumerPauseStub = sandbox.stub(kafkaStreamConsumer, 'pause');
    });
    after(() => {
        sandbox.restore();
    });

    describe('Testing init and the manageQueue by interval', () => {

        let intervalId;
        let interval = 1000;
        let thresholdMessages = 20;
        let functionWithDelay = new Promise((resolve, reject) => {
            setTimeout(() => {
                return resolve();
            }, 100);
        });

        after(() => {
            clearInterval(intervalId);
        });

        it('Successful init to inner async queues', () => {
            kafkaThrottlingManager = rewire('../src/throttling/kafkaThrottlingManager');
            intervalId = kafkaThrottlingManager.init(thresholdMessages, interval, ['TopicA', 'TopicB'], functionWithDelay,functionWithDelay);
            let queues = kafkaThrottlingManager.__get__('innerQueues');
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
            sandbox.resetHistory();
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

    describe('handleIncomingMessage method tests', () => {
        let intervalId;
        before(() => {
            commitFunctionStub = sandbox.stub();
            innerQueuePushStub = {
                push: sandbox.stub(),
                length: sandbox.stub()
            };

            asyncQueueStub = sandbox.stub(async, 'queue');
            asyncQueueStub.returns(innerQueuePushStub);
            kafkaThrottlingManager = rewire('../src/throttling/kafkaThrottlingManager');
            intervalId = kafkaThrottlingManager.init(1, 1, ['TopicA', 'TopicB'], () => Promise.resolve(), () => Promise.resolve(true));
        });
        afterEach(() => {
            sandbox.reset();
        });

        after(() => {
            clearInterval(intervalId);
        });

        it('First call to handleIncomingMessage should create the right partition-queue ', () => {
            let message = {
                topic: 'TopicA',
                partition: 4,
                msg: 'some-message'
            };
            kafkaThrottlingManager.handleIncomingMessage(message);
            should(asyncQueueStub.calledOnce).eql(true);
            should(innerQueuePushStub.push.calledOnce).eql(true);
            should(innerQueuePushStub.push.args[0][0]).eql(message);
        });
        it('Second call to handleIncomingMessage should write to inner queue ', () => {
            let message = {
                topic: 'TopicA',
                partition: 4,
                msg: 'some-message'
            };
            kafkaThrottlingManager.handleIncomingMessage(message);
            should(asyncQueueStub.called).eql(false);
            should(innerQueuePushStub.push.calledOnce).eql(true);
            should(innerQueuePushStub.push.args[0][0]).eql(message);
        });
    });
});