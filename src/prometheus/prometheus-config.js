
module.exports = {
    BUCKETS:{
        PROMETHEUS_KAFKA_DURATION_SIZES_BUCKETS: [0.001, 0.003, 0.005, 0.015, 0.03, 0.05, 0.1, 0.15, 0.2, 0.3, 0.4, 0.5]
    },
    METRIC_NAMES:{
        KAFKA_REQUEST_DURATION: 'kafka_request_duration_seconds',
        CONSUMER_GROUP_OFFSET: 'kafka_consumer_group_offset_diff'
    },
    START_TIMER_FACTOR: 1e3
};