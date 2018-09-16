let producer = require('./producers/kafkaProducer');
let kafkaConsumer = require('./consumers/kafkaConsumer');
let kafkaStreamConsumer = require('./consumers/kafkaStreamConsumer');

let dependencyChecker = require('./healthCheckers/dependencyChecker');
let chosenConsumer = {};
let _ = require('lodash');

function init(configuration) {
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

    chosenConsumer = configuration.AutoCommit === false ? kafkaStreamConsumer : kafkaConsumer;
    return producer.init(configuration)
        .then(() => {
            return chosenConsumer.init(configuration);
        }).then(() => {
            return dependencyChecker.init(chosenConsumer, configuration);
        });
}

function verifyParamIsFunction(param, paramName) {
    if (!(param && {}.toString.call(param) === '[object Function]')) {
        throw new Error(paramName + ' should be a valid function');
    }
}

module.exports = {
    init: init,
    validateOffsetsAreSynced: () => chosenConsumer.validateOffsetsAreSynced(),
    pause: () => chosenConsumer.pause(),
    resume: () => chosenConsumer.resume(),
    closeConnection: () => chosenConsumer.closeConnection(),
    finishedHandlingMessage: () => chosenConsumer.decreaseMessageInMemory(),
    send: producer.send,
    getLastMessage: chosenConsumer.getLastMessage

};