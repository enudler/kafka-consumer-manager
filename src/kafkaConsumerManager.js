let producer = require('./kafkaProducer');
let consumer = require('./kafkaConsumer');
let kafkaStreamConsumer = require('./kafkaStreamConsumer');

let healthChecker = require('./healthChecker');

let _ = require('lodash');

function init(configuration) {
    let mandatoryVars = [
        'KafkaUrl',
        'GroupId',
        'KafkaOffsetDiffThreshold',
        'KafkaConnectionTimeout',
        'Topics',
        'ResumePauseIntervalMs',
        'AutoCommit'
    ];

    if (configuration.AutoCommit === false) {
        mandatoryVars.push('ThrottlingThresholdPerQueue', 'ThrottlingCheckIntervalMs');
    }

    let missingFields = _.filter(mandatoryVars, (currVar) => {
        return !configuration.hasOwnProperty(currVar);
    });

    if (missingFields.length > 0) {
        throw new Error('Missing mandatory environment variables: ' + missingFields);
    }

    verifyParamIsFunction(configuration.ResumePauseCheckFunction, 'ResumePauseCheckFunction');
    verifyParamIsFunction(configuration.MessageFunction, 'MessageFunction');

    let specificConsumer = configuration.AutoCommit === false ? kafkaStreamConsumer : consumer;
    return producer.init(configuration)
        .then(() => {
            return specificConsumer.init(configuration);
        }).then(() => {
            return healthChecker.init(specificConsumer, configuration);
        });
}

function verifyParamIsFunction(param, paramName) {
    if (!(param && {}.toString.call(param) === '[object Function]')) {
        throw new Error(paramName + ' should be a valid function');
    }
}

module.exports = {
    init: init,
    healthCheck: consumer.healthCheck,
    pause: consumer.pause,
    resume: consumer.resume,
    closeConnection: consumer.closeConnection,
    send: producer.send
};