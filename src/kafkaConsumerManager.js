let KafkaProducer = require('./producers/kafkaProducer');
let KafkaConsumer = require('./consumers/kafkaConsumer');
let KafkaStreamConsumer = require('./consumers/kafkaStreamConsumer');
let DependencyChecker = require('./healthCheckers/dependencyChecker');
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

        Object.assign(this, {
            chosenConsumer: chosenConsumer,
            producer: new KafkaProducer(),
            dependencyChecker: new DependencyChecker()
        });

        this.dependencyChecker.init(chosenConsumer, configuration);

        return this.producer.init(configuration)
            .then(() => {
                return this.chosenConsumer.init(configuration);
            });
    }

    validateOffsetsAreSynced() {
        return this.chosenConsumer.validateOffsetsAreSynced();
    }

    pause(){
        return this.chosenConsumer.pause();
    }
    resume() {
        return this.chosenConsumer.resume();
    }

    closeConnection() {
        return this.chosenConsumer.closeConnection();
    }

    finishedHandlingMessage() {
        return this.chosenConsumer.decreaseMessageInMemory();
    }
    send() {
        return this.producer.send();
    }

    getLastMessage() {
        return this.chosenConsumer.getLastMessage();
    }
};

function verifyParamIsFunction(param, paramName) {
    if (!(param && {}.toString.call(param) === '[object Function]')) {
        throw new Error(paramName + ' should be a valid function');
    }
}
