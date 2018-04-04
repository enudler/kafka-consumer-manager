let producer = require('./producers/kafkaProducer');
let kafkaConsumer = require('./consumers/kafkaConsumer');
let kafkaStreamConsumer = require('./consumers/kafkaStreamConsumer');

let dependencyChecker = require('./healthCheckers/dependencyChecker');
let chosenConsumer = {};
let _ = require('lodash');

function init(configuration) {
    let mandatoryVars = [
        'KafkaUrl', // kafkaUrl have to be the first item.
        'GroupId',
        'KafkaOffsetDiffThreshold',
        'KafkaConnectionTimeout',
        'Topics',
        'AutoCommit'
    ];

    if (configuration.AutoCommit === false) {
        mandatoryVars.push('ThrottlingThreshold', 'ThrottlingCheckIntervalMs');
        mandatoryVars.shift(); // remove 'kafkaUrl' from mandatoryVars
        if (!configuration.hasOwnProperty('KafkaUrl') && !configuration.hasOwnProperty('ZookeeperUrl')) {
            throw new Error('Missing mandatory environment variables. one of the following: [KafkaUrl, ZookeeperUrl] should exist');
        }
        if (configuration.hasOwnProperty('KafkaUrl') && configuration.hasOwnProperty('ZookeeperUrl')) {
            throw new Error('Only one of the following: [KafkaUrl, ZookeeperUrl] should exist');
        }
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
    send: producer.send
};