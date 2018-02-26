'use-strict';
let async = require('async');
let commitFunction;
let innerQueues, callbackPromise;

function init(promise, commit) {
    innerQueues = {};
    callbackPromise = promise;
    commitFunction = commit;
}

function generateThrottlingQueueInstance() {
    let queue = async.queue(function (message, commitOffsetCallback) {
        return callbackPromise(message).then(() => {
            commitOffsetCallback(message);
        });
    }, 1);
    return queue;
}

function handleIncomingMessage(message) {
    // Todo support multiple queue per consumer
    let partition = message.partition;
    if (!innerQueues[partition]) {
        innerQueues[partition] = generateThrottlingQueueInstance();
    }
    innerQueues[partition].push(message, () => {
        commitFunction(message);
    });
}

function getQueueLengths() {
    let lengths = [];
    Object.keys(innerQueues).forEach((key) => {
        lengths.push(innerQueues[key].length());
    });
    return lengths;
}

module.exports = {
    init,
    handleIncomingMessage,
    getQueueLengths
};
