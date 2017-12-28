# kafka-consumer-manager

[![NPM](https://nodei.co/npm/kafka-consumer-manager.png?downloads=true&downloadRank=true&stars=true)](https://nodei.co/npm/kafka-consumer-manager/)
[![NPM](https://nodei.co/npm-dl/kafka-consumer-manager.png?months=1)](https://nodei.co/npm/kafka-consumer-manager/)


This package is used to to simplify the common use of kafka consumer by:
* Provides api for kafka consumer health check by checking that the offset of the partition is synced
* Accepts a callback function with the business logic each consumed message should go through
* Accepts a callback function with the business logic of when to pause and when to resume the consuming
* Provides api for sending message back to the topic (usually for retries)

## Install
```bash
npm install --save kafka-consumer-manager
```

## API

### How to use

```js
let kafkaConsumerManager = require('kafka-consumer-manager');
```

```js
let configuration = {
        KafkaUrl: "localhost:9092",
        GroupId: "some-group-id",
        KafkaConnectionTimeout: 10000,
        ZookeeperUrl: "localhost:2181",
        KafkaOffsetDiffThreshold: 3,
        Topics: ["TOPIC-A", "TOPIC-B"],
        ResumePauseIntervalMs: 30000,
        ResumePauseCheckFunction: () => {
            return shouldPauseConsuming()
        },
        MessageFunction: (msg) => { handleMessage(msg) }
    };

```

```js   
kafkaConsumerManager.init(configuration)
    .then(() => {})
```
##### Configuration

* `KafkaUrl` &ndash; URL of Kafka.
* `GroupId` &ndash; Defines the Consumer Group this process is consuming on behalf of.
* `KafkaConnectionTimeout` &ndash; Max wait time wait kafka to connect.
* `ZookeeperUrl` &ndash; Zookeeper URL.
* `KafkaOffsetDiffThreshold` &ndash; Tolerance for how far the partition offset of the consumer can be from the real offset, this value is used by the health check to reject in case the offset is out of sync.
* `Topics` &ndash; Array of topics that should be consumed.
* `ResumePauseIntervalMs` &ndash; Interval of when to run the ResumePauseCheckFunction.
* `ResumePauseCheckFunction` &ndash; Function that in case of return value is true, the consumer will be resumed, if false it will be paused.
* `MessageFunction` &ndash; Function that applied to each consumed message, this function accepts one param (message).

### kafka-consumer-manager.init(configuration)

Init the consumer and the producer, make sure to pass full configuration object else you will get exceptions.

The function returns Promise.

### kafka-consumer-manager.healthCheck()

Runs a health check checking the connection to kafka, also testing that offset is not out of sync according to KafkaOffsetDiffThreshold

The function returns Promise.

### kafka-consumer-manager.closeConnection()

Closes the connection to kafka.

### kafka-consumer-manager.pause()

Pause the consuming of new messages.

### kafka-consumer-manager.resume()

Resume the consuming of new messages.

### kafka-consumer-manager.retryMessage(message, topic)

Send a message back to a topic.

## Running Tests
Using mocha and istanbul 
```bash
npm test
```