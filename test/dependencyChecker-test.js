'use strict';

let sinon = require('sinon'),
    should = require('should'),
    DependencyChecker = require('../src/healthCheckers/dependencyChecker'),
    KafkaConsumer = require('../src/consumers/kafkaConsumer');

describe('Testing health checker', function () {
    let sandbox, consumerPauseStub, consumerResumeStub, healthChecker, logger, logErrorStub;
    let consumer = new KafkaConsumer();

    before(() => {
        sandbox = sinon.sandbox.create();
        sandbox.stub(consumer, 'init');
        logErrorStub = sandbox.stub();
        consumerPauseStub = sandbox.stub(consumer, 'pause');
        consumerResumeStub = sandbox.stub(consumer, 'resume');
        logger = {error: logErrorStub, trace: sandbox.stub(), info: sandbox.stub()};
    });

    after(() => {
        sandbox.restore();
    });

    afterEach(() => {
        healthChecker.stop();
        sandbox.reset();
    });

    it('Testing health checker is not configured - health checker disabled', (done) => {
        let configuration = {
        };

        healthChecker = new DependencyChecker();
        healthChecker.init(consumer, configuration, logger);
        setTimeout(() => {
            should(consumerResumeStub.called).eql(false);
            done();
        }, 100);
    });

    it('Testing health checker is up - one time check', (done) => {
        let configuration = {
            ResumePauseCheckFunction: (innerConsumer) => {
                should(consumer).eql(innerConsumer);
                return Promise.resolve(true);
            },
            ResumePauseIntervalMs: 50
        };

        healthChecker = new DependencyChecker();
        healthChecker.init(consumer, configuration, logger);
        setTimeout(() => {
            should(consumerResumeStub.calledOnce).eql(true);
            done();
        }, 75);
    });

    it('Testing health checker is up - three time check', (done) => {
        let configuration = {
            ResumePauseCheckFunction: () => {
                return Promise.resolve(true);
            },
            ResumePauseIntervalMs: 50
        };

        healthChecker = new DependencyChecker();
        healthChecker.init(consumer, configuration, logger);

        setTimeout(() => {
            should(consumerResumeStub.callCount).eql(3);
            done();
        }, 175);
    });

    it('Testing health checker is down - one time check', (done) => {
        let configuration = {
            ResumePauseCheckFunction: () => {
                return Promise.resolve(false);
            },
            ResumePauseIntervalMs: 50
        };

        healthChecker = new DependencyChecker();
        healthChecker.init(consumer, configuration, logger);

        setTimeout(() => {
            should(consumerPauseStub.calledOnce).eql(true);
            done();
        }, 75);
    });

    it('Testing health checker is down - three time check', (done) => {
        let configuration = {
            ResumePauseCheckFunction: () => {
                return Promise.resolve(false);
            },
            ResumePauseIntervalMs: 50
        };

        healthChecker = new DependencyChecker();
        healthChecker.init(consumer, configuration, logger);

        setTimeout(() => {
            should(consumerPauseStub.callCount).eql(3);
            done();
        }, 175);
    });

    it('Testing health checker is UP DOWN UP DOWN', (done) => {
        let index = 0;
        let configuration = {
            ResumePauseCheckFunction: () => {
                return Promise.resolve(index++ % 2 === 0);
            },
            ResumePauseIntervalMs: 50
        };

        healthChecker = new DependencyChecker();
        healthChecker.init(consumer, configuration, logger);

        setTimeout(() => {
            should(consumerResumeStub.callCount).eql(2);
            should(consumerPauseStub.callCount).eql(2);
            done();
        }, 225);
    });

    it('Testing health checker return reject', (done) => {
        let configuration = {
            ResumePauseCheckFunction: () => {
                return Promise.reject(new Error('some message'));
            },
            ResumePauseIntervalMs: 50
        };

        healthChecker = new DependencyChecker();
        healthChecker.init(consumer, configuration, logger);
        setTimeout(() => {
            should(consumerResumeStub.calledOnce).eql(false);
            should(logErrorStub.args[0]).eql([new Error('some message'), 'ResumePauseCheckFunction was rejected']);
            done();
        }, 75);
    });
});
