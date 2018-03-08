let logger = require('../helpers/logger'),
    configuration,
    intervalId;

function init(consumer, config) {
    if (!config.ResumePauseIntervalMs || !config.ResumePauseCheckFunction) {
        logger.info('No ResumePauseIntervalMs or ResumePauseCheckFunction set, functionality disabled');
        return;
    }
    configuration = config;
    intervalId = setInterval(() => {
        configuration.ResumePauseCheckFunction()
            .then((shouldResume) => {
                if (shouldResume) {
                    logger.info('ran ResumePauseCheckFunction and got should resume. will try to resume consumer if it was stopped');
                    consumer.setDependencyHealthy(true);
                    consumer.resume();
                } else {
                    logger.info('ran ResumePauseCheckFunction and got should pause, will pause consumer if it was running');
                    consumer.setDependencyHealthy(false);
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