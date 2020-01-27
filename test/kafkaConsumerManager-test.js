'use strict';

let sinon = require('sinon'),
    should = require('should'),
    KafkaConsumerManager = require('../src/kafkaConsumerManager'),
    KafkaProducer = require('../src/producers/kafkaProducer'),
    KafkaConsumer = require('../src/consumers/kafkaConsumer'),
    KafkaStreamConsumer = require('../src/consumers/kafkaStreamConsumer'),
    DependencyChecker = require('../src/healthCheckers/dependencyChecker'),
    KafkaThrottlingManager = require('../src/throttling/kafkaThrottlingManager'),
    prometheusUtils = require('../src/prometheus/prometheus-utils'),
    bunyan = require('bunyan'),
    _ = require('lodash');

let MandatoryFieldsConfiguration = {
    KafkaUrl: 'url',
    GroupId: 'GroupId',
    KafkaConnectionTimeout: '1000',
    KafkaOffsetDiffThreshold: '3',
    Topics: ['topic-a', 'topic-b'],
    AutoCommit: true,
    MessageFunction: (msg) => {
    }
};

let fullConfigurationCommitTrue = {
    LoggerName: 'test',
    KafkaUrl: 'url',
    GroupId: 'GroupId',
    KafkaConnectionTimeout: '1000',
    KafkaRequestTimeout: '3000',
    KafkaOffsetDiffThreshold: '3',
    Topics: ['topic-a', 'topic-b'],
    AutoCommit: true,
    MessageFunction: (msg) => {
    }
};

let fullConfigurationCommitFalse = {
    LoggerName: 'test',
    KafkaUrl: 'url',
    GroupId: 'GroupId',
    KafkaConnectionTimeout: '1000',
    KafkaRequestTimeout: '3000',
    KafkaOffsetDiffThreshold: '3',
    Topics: ['topic-a', 'topic-b'],
    AutoCommit: false,
    ThrottlingCheckIntervalMs: 1000,
    ThrottlingThreshold: 100,
    MessageFunction: (msg) => {
    }
};

describe('Verify mandatory params', () => {
    let sandbox, kafkaConsumerManager = new KafkaConsumerManager(), producerInitStub,
        consumerInitStub, dependencyInitStub, throttlingInitStub, loggerChildStub, consumerStreamInitStub;

    beforeEach(() => {
        producerInitStub.resolves();
        consumerInitStub.resolves();
        dependencyInitStub.returns({});
        throttlingInitStub.returns({});
    });

    before(() => {
        sandbox = sinon.sandbox.create();
        producerInitStub = sandbox.stub(KafkaProducer.prototype, 'init');
        consumerInitStub = sandbox.stub(KafkaConsumer.prototype, 'init');
        consumerStreamInitStub = sandbox.stub(KafkaStreamConsumer.prototype, 'init');
        dependencyInitStub = sandbox.stub(DependencyChecker.prototype, 'init');
        throttlingInitStub = sandbox.stub(KafkaThrottlingManager.prototype, 'init');
        loggerChildStub = sandbox.stub(bunyan.prototype, 'child');
    });

    afterEach(() => {
        sandbox.reset();
    });

    after(() => {
        sandbox.restore();
    });

    it('All params exists - kafkaConsumer', async () => {
        await kafkaConsumerManager.init(fullConfigurationCommitTrue);
        should(producerInitStub.calledOnce).eql(true);
        should(consumerInitStub.calledOnce).eql(true);
        should(consumerStreamInitStub.calledOnce).eql(false);
        should(dependencyInitStub.calledOnce).eql(true);
        should(loggerChildStub.calledOnce).eql(true);
        should(loggerChildStub.args[0][0]).eql({consumer_name: fullConfigurationCommitTrue.LoggerName});
    });

    it('All params exists - kafkaStreamConsumer', async () => {
        await kafkaConsumerManager.init(fullConfigurationCommitFalse);
        should(producerInitStub.calledOnce).eql(true);
        should(consumerInitStub.calledOnce).eql(false);
        should(consumerStreamInitStub.calledOnce).eql(true);
        should(dependencyInitStub.calledOnce).eql(true);
        should(loggerChildStub.calledOnce).eql(true);
        should(loggerChildStub.args[0][0]).eql({consumer_name: fullConfigurationCommitTrue.LoggerName});
    });

    it('All params are missing', async () => {
        let config = {};

        try {
            await kafkaConsumerManager.init(config);
            throw new Error('Should fail');
        } catch (err) {
            err.message.should.eql('Missing mandatory environment variables: KafkaUrl,GroupId,KafkaOffsetDiffThreshold,KafkaConnectionTimeout,Topics,AutoCommit');
        }
    });

    Object.keys(MandatoryFieldsConfiguration).forEach(key => {
        it('Test without ' + key + ' param', async () => {
            let clonedConfig = _.cloneDeep(MandatoryFieldsConfiguration);
            delete clonedConfig[key];

            try {
                await kafkaConsumerManager.init(clonedConfig);
                throw new Error('Should fail');
            } catch (err) {
                if (key.toUpperCase().indexOf('FUNCTION') > -1) {
                    err.message.should.eql(key + ' should be a valid function');
                } else {
                    err.message.should.eql('Missing mandatory environment variables: ' + key);
                }
            }
        });
    });

    it('ResumePauseIntervalMs exists without the ResumePauseCheckFunction should fail', async () => {
        let fullConfigurationWithPauseResume = JSON.parse(JSON.stringify(MandatoryFieldsConfiguration));
        fullConfigurationWithPauseResume.ResumePauseIntervalMs = 1000;
        try {
            await kafkaConsumerManager.init(fullConfigurationWithPauseResume);
            throw new Error('Should fail');
        } catch (err) {
            err.message.should.eql('ResumePauseCheckFunction should be a valid function');
        }
    });

    it('ResumePauseIntervalMs exists with the ResumePauseCheckFunction should success', async () => {
        let fullConfigurationWithPauseResume = JSON.parse(JSON.stringify(MandatoryFieldsConfiguration));
        fullConfigurationWithPauseResume.ResumePauseIntervalMs = 1000;
        fullConfigurationWithPauseResume.ResumePauseCheckFunction = () => {
            return Promise.resolve();
        };
        fullConfigurationWithPauseResume.MessageFunction = () => {
            return Promise.resolve();
        };
        await kafkaConsumerManager.init(fullConfigurationWithPauseResume);
    });

    it('CreateProducer equal false', async () => {
        let fullConfigurationWithoutProducer = _.cloneDeep(fullConfigurationCommitTrue);
        fullConfigurationWithoutProducer.CreateProducer = false;

        await kafkaConsumerManager.init(fullConfigurationWithoutProducer);
        should(producerInitStub.calledOnce).eql(false);
        should(consumerInitStub.calledOnce).eql(true);
        should(consumerStreamInitStub.calledOnce).eql(false);
        should(dependencyInitStub.calledOnce).eql(true);
        should(loggerChildStub.calledOnce).eql(true);
        should(loggerChildStub.args[0][0]).eql({consumer_name: fullConfigurationCommitTrue.LoggerName});
    });
});

describe('Verify export functions', () => {
    let sandbox, resumeStub, pauseStub, validateOffsetsAreSyncedStub,
        closeConnectionStub, decreaseMessageInMemoryStub, kafkaConsumerManager,
        dependencyInitStub, throttlingInitStub;

    before(async () => {
        kafkaConsumerManager = new KafkaConsumerManager();
        sandbox = sinon.sandbox.create();
        resumeStub = sandbox.stub();
        pauseStub = sandbox.stub();
        validateOffsetsAreSyncedStub = sandbox.stub();
        closeConnectionStub = sandbox.stub();
        decreaseMessageInMemoryStub = sandbox.stub();

        sandbox.stub(KafkaProducer.prototype, 'init').resolves();
        sandbox.stub(KafkaConsumer.prototype, 'init').resolves();

        dependencyInitStub = sandbox.stub(DependencyChecker.prototype, 'init');
        throttlingInitStub = sandbox.stub(KafkaThrottlingManager.prototype, 'init');

        await kafkaConsumerManager.init(MandatoryFieldsConfiguration);

        let consumer = {
            logger: {},
            resume: resumeStub,
            pause: pauseStub,
            validateOffsetsAreSynced: validateOffsetsAreSyncedStub,
            closeConnection: closeConnectionStub,
            decreaseMessageInMemory: decreaseMessageInMemoryStub
        };
        kafkaConsumerManager._chosenConsumer = consumer;
    });

    after(() => {
        sandbox.restore();
    });

    it('Verify methods going to the correct consumer', () => {
        kafkaConsumerManager.resume();
        should(resumeStub.calledOnce).eql(true);

        kafkaConsumerManager.pause();
        should(pauseStub.calledOnce).eql(true);

        kafkaConsumerManager.validateOffsetsAreSynced();
        should(validateOffsetsAreSyncedStub.calledOnce).eql(true);

        kafkaConsumerManager.closeConnection();
        should(closeConnectionStub.calledOnce).eql(true);

        kafkaConsumerManager.finishedHandlingMessage();
        should(decreaseMessageInMemoryStub.calledOnce).eql(true);
    });
});

describe('Verify emitter', () => {
    let EventEmitter = require('events').EventEmitter;
    let emitter = new EventEmitter();
    let sandbox;
    let kafkaConsumerManager = new KafkaConsumerManager();
    let producerInitStub,
        onStub,
        consumerInitStub, dependencyInitStub,
        throttlingInitStub,
        consumerOnStub,
        consumerStreamOnStub,
        loggerChildStub,
        consumerStreamInitStub;

    beforeEach(() => {
        producerInitStub.resolves();
        consumerInitStub.resolves();
        consumerStreamInitStub.resolves({});
        dependencyInitStub.returns({});
        throttlingInitStub.returns({});
    });

    before(() => {
        sandbox = sinon.sandbox.create();
        producerInitStub = sandbox.stub(KafkaProducer.prototype, 'init');
        consumerInitStub = sandbox.stub(KafkaConsumer.prototype, 'init');
        consumerOnStub = sandbox.stub(KafkaConsumer.prototype, 'on').callsFake((name, handler) => {
            emitter.on(name, handler);
        });
        consumerStreamOnStub = sandbox.stub(KafkaStreamConsumer.prototype, 'on').callsFake((name, handler) => {
            emitter.on(name, handler);
        });
        consumerStreamInitStub = sandbox.stub(KafkaStreamConsumer.prototype, 'init');
        dependencyInitStub = sandbox.stub(DependencyChecker.prototype, 'init');
        throttlingInitStub = sandbox.stub(KafkaThrottlingManager.prototype, 'init');
        loggerChildStub = sandbox.stub(bunyan.prototype, 'child');
        onStub = sandbox.stub();
    });

    afterEach(() => {
        sandbox.reset();
    });

    after(() => {
        sandbox.restore();
    });

    it('verify kafkaConsumer emitter', async () => {
        await kafkaConsumerManager.init(fullConfigurationCommitTrue);
        kafkaConsumerManager.on('error', onStub);
        let err = new Error('testError');
        emitter.emit('error', err);
        should(consumerOnStub.calledOnce).equal(true);
        should(consumerOnStub.args[0]).eql(['error', onStub]);
        should(onStub.calledOnce).equal(true);
        should(onStub.args[0][0]).eql(err);
    });

    it('verify kafkaStreamConsumer emitter', async () => {
        await kafkaConsumerManager.init(fullConfigurationCommitFalse);
        kafkaConsumerManager.on('error', onStub);
        let err = new Error('test');
        emitter.emit('error', err);
        should(consumerStreamOnStub.calledOnce).equal(true);
        should(consumerStreamOnStub.args[0]).eql(['error', onStub]);
        should(onStub.calledOnce).equal(true);
        should(onStub.args[0][0]).eql(err);
    });
});

describe('Verify metrics', () => {
    let sandbox;
    let kafkaConsumerManager = new KafkaConsumerManager();
    let producerInitStub,
        consumerInitStub, dependencyInitStub,
        throttlingInitStub,
        consumerGetOffsetCheckerStub,
        streamConsumerGetOffsetCheckerStub,
        registerOffsetGaugeObj,
        initConsumerGroupOffsetCheckerStub,
        initQueryHistogramStub,
        metricDecoratorStub,
        consumerStreamInitStub;

    beforeEach(() => {
        sandbox = sinon.sandbox.create();
        producerInitStub = sandbox.stub(KafkaProducer.prototype, 'init');
        consumerInitStub = sandbox.stub(KafkaConsumer.prototype, 'init');
        initConsumerGroupOffsetCheckerStub = sandbox.stub(prometheusUtils, 'initKafkaConsumerGroupOffsetGauge');
        initQueryHistogramStub = sandbox.stub(prometheusUtils, 'initKafkaQueryHistogram');
        metricDecoratorStub = sandbox.stub(prometheusUtils, 'prometheusMetricDecorator');
        registerOffsetGaugeObj = {
            registerOffsetGauge: sandbox.stub()
        };
        consumerGetOffsetCheckerStub = sandbox.stub(KafkaConsumer.prototype, 'getConsumerOffsetOutOfSyncChecker');
        consumerGetOffsetCheckerStub.returns(registerOffsetGaugeObj);

        streamConsumerGetOffsetCheckerStub = sandbox.stub(KafkaStreamConsumer.prototype, 'getConsumerOffsetOutOfSyncChecker');
        streamConsumerGetOffsetCheckerStub.returns(registerOffsetGaugeObj);
        consumerStreamInitStub = sandbox.stub(KafkaStreamConsumer.prototype, 'init');
        dependencyInitStub = sandbox.stub(DependencyChecker.prototype, 'init');
        throttlingInitStub = sandbox.stub(KafkaThrottlingManager.prototype, 'init');
        producerInitStub.resolves();
        consumerInitStub.resolves();
        consumerStreamInitStub.resolves({});
        dependencyInitStub.returns({});
        throttlingInitStub.returns({});
    });

    afterEach(() => {
        sandbox.restore();
    });

    after(() => {
        sandbox.restore();
    });
    it('verify right metrics init, autoCommit = true', async () => {
        let conf = _.cloneDeep(fullConfigurationCommitTrue);
        conf.ExposePrometheusMetrics = true;
        initConsumerGroupOffsetCheckerStub.returns('kafkaConsumerGroupOffset');
        await kafkaConsumerManager.init(conf);
        should(prometheusUtils.initKafkaConsumerGroupOffsetGauge.calledOnce).equal(true);
        should(prometheusUtils.initKafkaConsumerGroupOffsetGauge.args[0].length).eql(0);
        should(registerOffsetGaugeObj.registerOffsetGauge.calledOnce).equal(true);
        should(consumerGetOffsetCheckerStub.calledOnce).equal(true);
        should(registerOffsetGaugeObj.registerOffsetGauge.args[0][0]).eql('kafkaConsumerGroupOffset');
        should(registerOffsetGaugeObj.registerOffsetGauge.args[0][1]).eql(5000);
        should(initQueryHistogramStub.notCalled).equal(true);
        should(metricDecoratorStub.notCalled).equal(true);
    });

    it('verify right metrics init, autoCommit = false', async () => {
        let conf = _.cloneDeep(fullConfigurationCommitFalse);
        conf.ExposePrometheusMetrics = true;
        initConsumerGroupOffsetCheckerStub.returns('kafkaConsumerGroupOffset');
        initQueryHistogramStub.returns('kafkaQueryHistogram');
        await kafkaConsumerManager.init(conf);
        should(prometheusUtils.initKafkaConsumerGroupOffsetGauge.calledOnce).equal(true);
        should(prometheusUtils.initKafkaConsumerGroupOffsetGauge.args[0].length).eql(0);
        should(registerOffsetGaugeObj.registerOffsetGauge.calledOnce).equal(true);
        should(streamConsumerGetOffsetCheckerStub.calledOnce).equal(true);
        should(registerOffsetGaugeObj.registerOffsetGauge.args[0][0]).eql('kafkaConsumerGroupOffset');
        should(registerOffsetGaugeObj.registerOffsetGauge.args[0][1]).eql(5000);
        should(initQueryHistogramStub.calledOnce).equal(true);
        should(metricDecoratorStub.calledOnce).equal(true);
        should(metricDecoratorStub.args[0][0]).eql(fullConfigurationCommitFalse.MessageFunction);
        should(metricDecoratorStub.args[0][1]).eql('kafkaQueryHistogram');
    });

    it('verify right metrics init, autoCommit = false, diff interval = 8000', async () => {
        let conf = _.cloneDeep(fullConfigurationCommitFalse);
        conf.ExposePrometheusMetrics = true;
        conf.ConsumerGroupOffsetCheckerInterval = 8000;
        initConsumerGroupOffsetCheckerStub.returns('kafkaConsumerGroupOffset');
        initQueryHistogramStub.returns('kafkaQueryHistogram');
        await kafkaConsumerManager.init(conf);
        should(prometheusUtils.initKafkaConsumerGroupOffsetGauge.calledOnce).equal(true);
        should(prometheusUtils.initKafkaConsumerGroupOffsetGauge.args[0].length).eql(0);
        should(registerOffsetGaugeObj.registerOffsetGauge.calledOnce).equal(true);
        should(streamConsumerGetOffsetCheckerStub.calledOnce).equal(true);
        should(registerOffsetGaugeObj.registerOffsetGauge.args[0][0]).eql('kafkaConsumerGroupOffset');
        should(registerOffsetGaugeObj.registerOffsetGauge.args[0][1]).eql(8000);
        should(initQueryHistogramStub.calledOnce).equal(true);
        should(metricDecoratorStub.calledOnce).equal(true);
        should(metricDecoratorStub.args[0][0]).eql(fullConfigurationCommitFalse.MessageFunction);
        should(metricDecoratorStub.args[0][1]).eql('kafkaQueryHistogram');
    });
});