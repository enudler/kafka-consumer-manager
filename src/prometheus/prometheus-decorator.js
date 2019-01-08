function prometheusMetricDecorator(originalFunction) {
    return (message) => {
        message.end = message.histogramMetic.startTimer({topic: message.topic});
        return originalFunction(message)
            .then((res) => {
                message.end({status: 'success'});
                return Promise.resolve(res);
            })
            .catch((err) => {
                message.end({status: 'failed'});
                return Promise.reject(err);
            });
    };
}

module.exports = prometheusMetricDecorator;
