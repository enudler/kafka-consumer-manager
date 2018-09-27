let KafkaProducer = require('./producers/kafkaProducer');
let KafkaConsumer = require('./consumers/kafkaConsumer');
let KafkaStreamConsumer = require('./consumers/kafkaStreamConsumer');
let DependencyChecker = require('./healthCheckers/dependencyChecker');
let bunyanLogger = require('./helpers/logger');
let _ = require('lodash');

module.exports = class KafkaConsumerManager {
    init(configuration) {
        let mandatoryVars = [
            'KafkaUrl',
            'GroupId',
            'KafkaOffsetDiffThreshold',
            'KafkaConnectionTimeout',
            'Topics',
            'AutoCommit'
        ];

        if (configuration.AutoCommit === false) {
            mandatoryVars.push('ThrottlingThreshold', 'ThrottlingCheckIntervalMs');
        }

        let missingFields = _.filter(mandatoryVars, (currVar) => {
            return !configuration.hasOwnProperty(currVar);
        });

        if (missingFields.length > 0) {
            throw new Error('Missing mandatory environment variables: ' + missingFields);
        }

        if (configuration.hasOwnProperty('ResumePauseIntervalMs')) {
            verifyParamIsFunction(configuration.ResumePauseCheckFunction, 'ResumePauseCheckFunction');
        }

        verifyParamIsFunction(configuration.MessageFunction, 'MessageFunction');

        let chosenConsumer = configuration.AutoCommit === false ? new KafkaStreamConsumer() : new KafkaConsumer();

        let loggerChild = {consumer_name: configuration.LoggerName} || {};
        let logger = bunyanLogger.child(loggerChild);

        Object.assign(this, {
            _logger: logger,
            _chosenConsumer: chosenConsumer,
            _producer: new KafkaProducer(),
            _dependencyChecker: new DependencyChecker()
        });

        this._dependencyChecker.init(chosenConsumer, configuration, logger);

        return this._producer.init(configuration, logger)
            .then(() => {
                return this._chosenConsumer.init(configuration, logger);
            });
    }

    validateOffsetsAreSynced() {
        return this._chosenConsumer.validateOffsetsAreSynced();
    }

    pause(){
        return this._chosenConsumer.pause();
    }
    resume() {
        return this._chosenConsumer.resume();
    }

    closeConnection() {
        this._dependencyChecker.stop();
        return this._chosenConsumer.closeConnection();
    }

    finishedHandlingMessage() {
        return this._chosenConsumer.decreaseMessageInMemory();
    }
    send() {
        return this._producer.send();
    }

    getLastMessage() {
        return this._chosenConsumer.getLastMessage();
    }
};

function verifyParamIsFunction(param, paramName) {
    if (!(param && {}.toString.call(param) === '[object Function]')) {
        throw new Error(paramName + ' should be a valid function');
    }
}
