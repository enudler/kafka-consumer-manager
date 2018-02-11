let logger = require('./logger'),
    consumer = require('./kafkaConsumer');

let configuration;
let intervalId;

function init(config) {
    if (!config.ResumePauseIntervalMs || !config.ResumePauseCheckFunction) {
        logger.info('No ResumePauseIntervalMs or ResumePauseCheckFunction set, functionality disabled');
        return;
    }
    configuration = config;
    intervalId = setInterval(() => {
        configuration.ResumePauseCheckFunction()
            .then((isHealthy) => {
                if (isHealthy) {
                    logger.info('ran health check and got health OK, will resume consumer if it was stopped');
                    consumer.resume();
                } else {
                    logger.info('ran health check and got health DOWN, will pause consumer if it was running');
                    consumer.pause();
                }
            });
    }, configuration.ResumePauseIntervalMs);

    return Promise.resolve();
}

function stop() {
    clearInterval(intervalId);
}

module.exports = {
    init: init,
    stop: stop
};