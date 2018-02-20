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
    let queue;

    // TODO: change back to:
    // Check what happens if not resolved
    // return callbackPromise(task.msg).then(() => {   -> so user's function won't affect offset data
    queue = async.queue(function (task, commitOffsetCallback) {
        return callbackPromise(task).then(() => {
            commitOffsetCallback();
        });
    }, 1);
    return queue;
}

function handleIncomingMessage(partition, msg) {
    // console.log('handling incoming message:' + msg.msg);
    if (innerQueues[partition]) {
        innerQueues[partition].push(msg, () => {
            commitFunction(msg.partition, msg.offset);
        });
    } else {
        innerQueues[partition] = generateThrottlingQueueInstance();
        innerQueues[partition].push(msg, () => {
            commitFunction(msg.partition, msg.offset);
        });
    }
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
