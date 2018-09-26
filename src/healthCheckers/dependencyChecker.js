let logger = require('../helpers/logger');

module.exports = class DependencyChecker {
    init(consumer, config){
        if (!config.ResumePauseIntervalMs || !config.ResumePauseCheckFunction) {
            logger.info('No ResumePauseIntervalMs or ResumePauseCheckFunction set, functionality disabled');
            return;
        }

        Object.assign(this, {
            resumePauseCheckFunction: config.ResumePauseCheckFunction,
            resumePauseIntervalMs: config.ResumePauseIntervalMs,
            consumer: consumer
        });

        this.intervalId = setInterval(function(){
            this.resumePauseCheckFunction(this.consumer)
                .then((shouldResume) => {
                    if (shouldResume) {
                        logger.info('ran ResumePauseCheckFunction and got should resume. will try to resume consumer if it was stopped');
                        this.consumer.setDependencyHealthy(true);
                        this.consumer.resume();
                    } else {
                        logger.info('ran ResumePauseCheckFunction and got should pause, will pause consumer if it was running');
                        this.consumer.setDependencyHealthy(false);
                        this.consumer.pause();
                    }
                });
        }.bind(this), this.resumePauseIntervalMs);
    }

    stop() {
        clearInterval(this.intervalId);
    }
};
