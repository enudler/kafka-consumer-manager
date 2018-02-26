let sinon = require('sinon'),
    rewire = require('rewire'),
    throttlingQueue = rewire('../src/throttlingInternalQueues');

let sandbox, commitFunctionStub, generateThrottlingQueueInstanceStub, innerQueuePushStub, innerQueuesStubsObj;

describe('Testing throttlingInternalQueues component', function () {
    before(function () {
        sandbox = sinon.sandbox.create();
    });

    after(function () {
        sandbox.restore();
    });
    describe('handleIncomingMessage method tests', function () {
        before(() => {
            commitFunctionStub = sandbox.stub();
            throttlingQueue.init(Promise.resolve(), commitFunctionStub);
        });
        afterEach(function () {
            sandbox.reset();
        });

        it('First call to handleIncomingMessage should create the right partition-queue ', function () {
            commitFunctionStub = sandbox.stub();
            innerQueuePushStub = {
                push: sandbox.stub()
            };
            generateThrottlingQueueInstanceStub = sandbox.stub();
            generateThrottlingQueueInstanceStub.returns(innerQueuePushStub);
            throttlingQueue.__set__('generateThrottlingQueueInstance', () => {
                return innerQueuePushStub;
            });

            let message = {
                partition: 4,
                msg: 'some-message'
            };
            throttlingQueue.handleIncomingMessage(message);
            sinon.assert.calledOnce(innerQueuePushStub.push);
            sinon.assert.calledWith(innerQueuePushStub.push, message);
        });
        it('Second call to handleIncomingMessage should write to inner queue ', function () {
            // Todo missing assertions for create / not create new queue
            let message = {
                partition: 4,
                msg: 'some-message'
            };
            throttlingQueue.handleIncomingMessage(message);
            sinon.assert.calledOnce(innerQueuePushStub.push);
            sinon.assert.calledWith(innerQueuePushStub.push, message);
        });
    });
});