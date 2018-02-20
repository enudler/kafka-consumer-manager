const express = require('express');
var bodyParser = require('body-parser');
var kafkaManager = require('./kafkaConsumerManager');
var rp = require('request-promise');
var metrics = require('express-node-metrics');

var options = {
    method: 'POST',
    uri: 'http://localhost:5554',
    json: true
};
let configuration = {
    KafkaUrl: 'localhost:9092',
    GroupId: 'group-id',
    KafkaConnectionTimeout: 10000,
    flowManagerInterval: 10000,
    KafkaOffsetDiffThreshold: 3,
    Topics: ['T3'],
    ResumePauseIntervalMs: 30000,
    throttling: true,
    ResumePauseCheckFunction: () => {
        return true;
    },
    MessageFunction: (msg) => {
        console.log(`got message: ${msg}`);
    },
    MessagePromise: (queueMsg) => {
        return new Promise((resolve, reject) => {
            // doing some work...
            // console.log('#################\nrunning Promise on msg: ' + msg + '\n#################');

            options.body = {
                msg: queueMsg.msg,
                partition: queueMsg.partition
            };
            return rp(options)
                .then(() => {
                    console.log('posted:\n' + options.body.msg);
                    resolve();
                });
            // finished work!
        });
    }
};
const app = express();

app.use(bodyParser.json());
app.post('/', (req, res) => {
    console.log('body is: ' + JSON.stringify(req.body));
    kafkaManager.send(req.body.msg, 'T3');
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