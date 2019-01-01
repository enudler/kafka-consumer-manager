const express = require('express');
let bodyParser = require('body-parser');
// let prometheus = require('prom-client');
let KafkaManager = require('../src/kafkaConsumerManager');
let kafkaManager = new KafkaManager();

let configuration = {
    KafkaUrl: 'localhost:9092',
    GroupId: 'group-id',
    KafkaConnectionTimeout: 10000,
    flowManagerInterval: 10000,
    KafkaOffsetDiffThreshold: 3,
    Topics: ['A', 'B'],
    ResumePauseIntervalMs: 30000,
    AutoCommit: false,
    ThrottlingThreshold: 25,
    ThrottlingCheckIntervalMs: 10000,

    ResumePauseCheckFunction: () => {
        return Promise.resolve(true);
    },
    MessageFunction: (message) => {
        return new Promise((resolve, reject) => {
            setTimeout(() => {
                console.log(`handling message ${JSON.stringify(message)}`)
                if (JSON.parse(message.value).message === 'fail') {
                    return reject();
                }
                return resolve();
            }, 500);
        });
    }
};
const app = express();

app.use(bodyParser.json());

app.post('/', (req, res) => {
    console.log('body is: ' + JSON.stringify(req.body));
    kafkaManager.send(JSON.stringify({hello: 'keya'}), 'A');
    kafkaManager.send(JSON.stringify({hello: 'keyb'}), 'B');

    res.status(200);
    res.json(req.body);
});
app.post('/failure', (req, res) => {
    console.log('body is: ' + JSON.stringify(req.body));
    kafkaManager.send(JSON.stringify({message: 'fail'}), 'A');

    res.status(200);
    res.json(req.body);
});
setInterval(() => kafkaManager.validateOffsetsAreSynced(), 10000);
app.get('/metrics', (req, res) => {
    res.set('Content-Type', prometheus.register.contentType);
    setTimeout(() => {
        return res.end(prometheus.register.metrics());
    }, 0);
});

kafkaManager.init(configuration)
    .then(() => {
        app.listen(5555, () => console.log('queue-testing app listening on port 5555!'));
    })
    .catch((err) => {
        console.log(err);
        process.exit(1);
    });
