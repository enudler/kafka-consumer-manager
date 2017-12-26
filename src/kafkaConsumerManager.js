let producer = require('./kafkaProducer');
let consumer = require('./kafkaConsumer');
let healthChecker = require('./healthChecker');

let _ = require('lodash');

const MANDATORY_VARS = [
    'KAFKA_URL',
    'GROUP_ID',
    'ZOOKEEPER_URL',
    'KAFKA_OFFSET_DIFF_THRESHOLD',
    'KAFKA_CONNECTION_TIMEOUT',
    'TOPICS',
    'RESUME_PAUSE_INTERVAL_MS'
];

async function init(configuration) {
    let missingFields = _.filter(MANDATORY_VARS, (currVar) => {
        return !configuration[currVar];
    });

    if (missingFields.length > 0) {
        throw new Error('Missing mandatory environment variables: ' + missingFields);
    }

    verifyParamIsFunction(configuration.RESUME_PAUSE_CHECK_FUNCTION, 'RESUME_PAUSE_CHECK_FUNCTION');
    verifyParamIsFunction(configuration.MESSAGE_FUNCTION, 'MESSAGE_FUNCTION');

    await producer.init(configuration);
    await consumer.init(configuration);
    healthChecker.init(configuration);
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
    retryMessage: producer.retryMessage
};