'use-strict';

let async = require('async'),
    logger = require('../helpers/logger');
let messagesInMemoryThreshold, commitFunction, innerQueues, callbackPromise, kafkaConsumer;

function init(threshold, interval, topics, promise, commit) {
    kafkaConsumer = require('../consumers/kafkaStreamConsumer');
    messagesInMemoryThreshold = threshold;
    let intervalId = setInterval(() => {
        manageQueues();
    }, interval);

    innerQueues = {};
    topics.forEach(topic => {
        innerQueues[topic] = {};
    });
    callbackPromise = promise;
    commitFunction = commit;
    return intervalId;
}

function generateThrottlingQueueInstance() {
    let queue = async.queue(function (message, commitOffsetCallback) {
        return callbackPromise(message).then(() => {
            logger.trace(`kafkaThrottlingManager finished handling message: topic: ${message.topic}, partition: ${message.partition}, offset: ${message.offset}`);
            commitOffsetCallback(message);
        });
    }, 1);
    return queue;
}

function handleIncomingMessage(message) {
    let partition = message.partition;
    let topic = message.topic;
    if (!innerQueues[topic][partition]) {
        innerQueues[topic][partition] = generateThrottlingQueueInstance();
    }
    innerQueues[topic][partition].push(message, () => {
        commitFunction(message);
    });

}

function getQueueLengthsByTopic() {
    let queueSizesByTopic = {};
    Object.keys(innerQueues).forEach(topic => {
        let specificTopicQueueSizes = [];
        Object.keys(innerQueues[topic]).forEach((key) => {
            specificTopicQueueSizes.push(innerQueues[topic][key].length());
        });
        queueSizesByTopic[topic] = specificTopicQueueSizes;
    });
    return queueSizesByTopic;
}

function manageQueues() {
    logger.trace('managing queues..');
    let lengthsByTopic = getQueueLengthsByTopic();
    let totalMessagesInQueues = 0;
    Object.keys(lengthsByTopic).forEach(topic => {
        totalMessagesInQueues += lengthsByTopic[topic].reduce((a, b) => a + b, 0);
    });

    logger.trace('Total messages in queues are: ' + totalMessagesInQueues);
    let shouldResume = totalMessagesInQueues < messagesInMemoryThreshold;
    kafkaConsumer.setThirsty(shouldResume);
    shouldResume ? kafkaConsumer.resume() : kafkaConsumer.pause();
}

module.exports = {
    init,
    handleIncomingMessage
};