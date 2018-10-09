module.exports = class DependencyChecker {
    init(consumer, config, logger){
        if (!config.ResumePauseIntervalMs || !config.ResumePauseCheckFunction) {
            logger.info('No ResumePauseIntervalMs or ResumePauseCheckFunction set, functionality disabled');
            return;
        }

        Object.assign(this, {
            logger: logger,
            resumePauseCheckFunction: config.ResumePauseCheckFunction,
            resumePauseIntervalMs: config.ResumePauseIntervalMs,
            consumer: consumer,
            state: true,
            keepHandleResumePause: true
        });

        setTimeout(handleResumePauseFunc, this.resumePauseIntervalMs, this);
    }

    stop() {
        this.keepHandleResumePause = false;
    }
};

let handleResumePauseFunc = function(depChecker){
    if (depChecker.keepHandleResumePause) {
        depChecker.resumePauseCheckFunction(depChecker.consumer)
            .then((shouldResume) => {
                if (shouldResume) {
                    depChecker.logger.trace('ran ResumePauseCheckFunction and got should resume. will try to resume consumer if it was stopped');
                    depChecker.consumer.setDependencyHealthy(true);
                    depChecker.consumer.resume();
                } else {
                    depChecker.logger.trace('ran ResumePauseCheckFunction and got should pause, will pause consumer if it was running');
                    depChecker.consumer.setDependencyHealthy(false);
                    depChecker.consumer.pause();
                }
            })
            .catch(err => {
                depChecker.logger.error(err, 'ResumePauseCheckFunction was rejected');
            }).then(() => { // finally
                setTimeout(handleResumePauseFunc, depChecker.resumePauseIntervalMs, depChecker);
            });
    }
};