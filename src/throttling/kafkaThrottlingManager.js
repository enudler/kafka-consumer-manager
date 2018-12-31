'use-strict';

let async = require('async');

module.exports = class KafkaThrottlingManager {
    init(messagesInMemoryThreshold, interval, topics, callbackPromise, kafkaStreamConsumer, logger){
        Object.assign(this, {
            logger: logger,
            kafkaStreamConsumer: kafkaStreamConsumer,
            messagesInMemoryThreshold: messagesInMemoryThreshold,
            callbackPromise: callbackPromise,
            innerQueues: {}
        });
        topics.forEach(topic => {
            this.innerQueues[topic] = {};
        });

        this.intervalId = setInterval(function(){
            manageQueues(this.kafkaStreamConsumer, this.messagesInMemoryThreshold, this.innerQueues, this.logger);
        }.bind(this), interval);
    }

    handleIncomingMessage(message, histogramMetric){
        message.histogramMetic = histogramMetric;
        let partition = message.partition;
        let topic = message.topic;
        if (!this.innerQueues[topic][partition]) {
            this.innerQueues[topic][partition] = generateThrottlingQueueInstance(this.callbackPromise, this.logger);
        }
        this.innerQueues[topic][partition].push(message, () => {
            this.kafkaStreamConsumer.commit(message);
        });
    }

    stop(){
        clearInterval(this.intervalId);
    }
};

function generateThrottlingQueueInstance(callbackPromise, logger) {
    let queue = async.queue(function (message, commitOffsetCallback) {
        message.end = message.histogramMetic.startTimer({topic: 'TOPIC'});
        return callbackPromise(message).then(() => {
            message.end({status: 'success'})
            this.logger.trace(`kafkaThrottlingManager finished handling message: topic: ${message.topic}, partition: ${message.partition}, offset: ${message.offset}`);
            commitOffsetCallback(message);
        }).catch((err) => {
            message.end({status: 'failed'})
            this.logger.error(err, 'MessageFunction was rejected');
        });
    }.bind({logger}), 1);
    return queue;
}

function getQueueLengthsByTopic(innerQueues) {
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

function manageQueues(kafkaStreamConsumer, messagesInMemoryThreshold, innerQueues, logger) {
    logger.trace('managing queues..');
    let lengthsByTopic = getQueueLengthsByTopic(innerQueues);
    let totalMessagesInQueues = 0;
    Object.keys(lengthsByTopic).forEach(topic => {
        totalMessagesInQueues += lengthsByTopic[topic].reduce((a, b) => a + b, 0);
    });

    logger.trace('Total messages in queues are: ' + totalMessagesInQueues);
    let shouldResume = totalMessagesInQueues < messagesInMemoryThreshold;
    kafkaStreamConsumer.setThirsty(shouldResume);
    shouldResume ? kafkaStreamConsumer.resume() : kafkaStreamConsumer.pause();
}