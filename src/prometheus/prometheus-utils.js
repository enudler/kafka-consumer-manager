let prometheus = require('prom-client');
let prometheusConfig =
    require('./prometheus-config');

function prometheusMetricDecorator(originalFunction, histogramMetric) {
    return (message) => {
        message.end = histogramMetric.startTimer({topic: message.topic});
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

function initKafkaConsumerGroupOffsetGauge() {
    return new prometheus.Gauge({
        name: prometheusConfig.METRIC_NAMES.CONSUMER_GROUP_OFFSET,
        help: 'The service\'s consumer groups offset',
        labelNames: ['topic', 'consumer_group', 'partition']
    });
}

function initKafkaQueryHistogram(histogramBuckets) {
    return new prometheus.Histogram({
        name: prometheusConfig.METRIC_NAMES.KAFKA_REQUEST_DURATION,
        help: 'The duration time of processing kafka specific message',
        labelNames: ['status', 'topic'],
        buckets: histogramBuckets || prometheusConfig.BUCKETS.PROMETHEUS_KAFKA_DURATION_SIZES_BUCKETS
    });
}

module.exports = {
    prometheusMetricDecorator,
    initKafkaConsumerGroupOffsetGauge,
    initKafkaQueryHistogram
};
