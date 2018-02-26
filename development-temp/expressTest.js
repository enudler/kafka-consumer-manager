const express = require('express');
let bodyParser = require('body-parser');
let kafkaManager = require('../src/kafkaConsumerManager');
let rp = require('request-promise');
let metrics = require('express-node-metrics');
let logger = require('../src/logger');
let configuration = {
    KafkaUrl: 'localhost:9092',
    GroupId: 'group-id',
    KafkaConnectionTimeout: 10000,
    flowManagerInterval: 10000,
    KafkaOffsetDiffThreshold: 3,
    Topics: ['abcccc'],
    ResumePauseIntervalMs: 30000,
    AutoCommit: false,
    ThrottlingThresholdPerQueue: 3,
    ThrottlingCheckIntervalMs: 10000,

    ResumePauseCheckFunction: () => {
        return Promise.resolve(true);
    },
    MessageFunction: (message) => {
        return new Promise((resolve, reject) => {
            setTimeout(() => {
                logger.trace(`finished handling message: topic: ${message.topic}, partition: ${message.partition}, offset: ${message.offset}`);
                return resolve();
            }, 500);
        });
    }
};
const app = express();

app.use(bodyParser.json());
app.post('/', (req, res) => {
    console.log('body is: ' + JSON.stringify(req.body));
    kafkaManager.send(JSON.stringify({hello: 'key'}), 'abcccc');
    res.status(200);
    res.json(req.body);
});

app.get('/metrics', (req, res) => {
    return res.json(JSON.parse(metrics.metrics.getAll(true)));
});

kafkaManager.init(configuration)
    .then(() => {
        app.listen(5555, () => console.log('queue-testing app listening on port 5555!'));
    })
    .catch((err) => {
        console.log(err);
        process.exit(1);
    });