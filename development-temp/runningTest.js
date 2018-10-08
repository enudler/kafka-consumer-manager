'use-strict';
let KafkaConsumerManager = require('../src/kafkaConsumerManager');
let manager = new KafkaConsumerManager();

let configuration = {
    KafkaUrl: 'localhost:9092',
    GroupId: 'group-id',
    KafkaConnectionTimeout: 5000,
    KafkaOffsetDiffThreshold: 3,
    Topics: ['kafka-test3'],
    AutoCommit: false,
    ResumePauseIntervalMs: 30000,
    ThrottlingThreshold: 100,
    ThrottlingCheckIntervalMs: 1000,
    ResumePauseCheckFunction: () => {
        return true;
    },
    MessageFunction: (msg) => {
        console.log(`got message: ${msg}`);
    },
    MessagePromise: (msg) => {
        return new Promise((resolve, reject) => {
            // doing some work...
            console.log('#################\nrunning Promise on msg: ' + msg + '\n#################');
            // finished work!
            resolve();
        });
    }
};

function run() {
    console.log('initializing queues');
    return manager.init(configuration)
        .then(() => {
            console.log('queues initialized');
            let array = Array.from(Array(10).keys());
            array.forEach((key) => {
                console.log('sended:\nI am the Walrus! ' + key);
                manager.send('I am the Walrus! ' + key, 'kafka-test3');
            });
        })
        .catch((err) => {
            console.log('queues failed initializtion' + err);
        });
}

setTimeout(run, 3000);