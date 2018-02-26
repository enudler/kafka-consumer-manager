'use-strict';

let kafkaConsumer;
let throttlingQueues = require('./throttlingInternalQueues'),
    logger = require('./logger');

let messagePerQueueThreshold;

function init(threshold, interval) {
    kafkaConsumer = require('./kafkaStreamConsumer');
    messagePerQueueThreshold = threshold;
    setInterval(() => {
        manageQueues();
    }, interval);
}

function manageQueues() {
    logger.trace('managing queues..');
    let lengths = throttlingQueues.getQueueLengths();
    logger.trace('Inner queues lengths are: ' + lengths);
    if (lengths.length < 1) {
        return;
    }
    let sum = 0,
        shouldResume = false,
        msgPerQueue;
    lengths.forEach((queueLength) => {
        if (queueLength === 0) {
            shouldResume = true;
        }
        sum += queueLength;
    });
    if (shouldResume) {
        logger.info('found idle queue, resuming kafka..');
        return kafkaConsumer.resume();
    }
    msgPerQueue = sum / lengths.length;
    logger.info(`calculated msgPerQueue: ${msgPerQueue}`);
    msgPerQueue < messagePerQueueThreshold ? kafkaConsumer.resume() : kafkaConsumer.pause();
}

module.exports = {
    init
};