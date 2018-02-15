let producer = require('./kafkaProducer');
let consumer = require('./kafkaConsumer');
let healthChecker = require('./healthChecker');

let _ = require('lodash');

const MANDATORY_VARS = [
    'KafkaUrl',
    'GroupId',
    'KafkaOffsetDiffThreshold',
    'KafkaConnectionTimeout',
    'Topics',
    'ResumePauseIntervalMs'
];

function init(configuration) {
    let missingFields = _.filter(MANDATORY_VARS, (currVar) => {
        return !configuration[currVar];
    });

    if (missingFields.length > 0) {
        throw new Error('Missing mandatory environment variables: ' + missingFields);
    }

    verifyParamIsFunction(configuration.ResumePauseCheckFunction, 'ResumePauseCheckFunction');
    verifyParamIsFunction(configuration.MessageFunction, 'MessageFunction');

    if (!configuration.hasOwnProperty('throttling')) {
        configuration['throttling'] = false;
    }
    if (!configuration.hasOwnProperty('throttlingThreshold')) {
        configuration['throttlingThreshold'] = 300;
    }
    if (!configuration.hasOwnProperty('flowManagerInterval')) {
        configuration['flowManagerInterval'] = 1000;
    }
    return producer.init(configuration)
        .then(() => {
            return consumer.init(configuration);
        }).then(() => {
            // healthChecker.init(configuration);
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