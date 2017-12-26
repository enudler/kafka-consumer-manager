# kafka-consumer-manager

[![NPM](https://nodei.co/npm/kafka-consumer-manager.png)](https://nodei.co/npm/kafka-consumer-manager/)

[![NPM](https://nodei.co/npm-dl/kafka-consumer-manager.png?height=3)](https://nodei.co/npm/kafka-consumer-manager/)

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
        KAFKA_URL: "localhost:9092",
        GROUP_ID: "some-group-id",
        KAFKA_CONNECTION_TIMEOUT: 10000,
        ZOOKEEPER_URL: "localhost:2181",
        KAFKA_OFFSET_DIFF_THRESHOLD: 3,
        TOPICS: ["TOPIC-A", "TOPIC-B"],
        RESUME_PAUSE_INTERVAL_MS: 30000,
        RESUME_PAUSE_CHECK_FUNCTION: () => {
            return true;
        },
        MESSAGE_FUNCTION: (msg) => { handleMessage(msg) }
    };

```

```js   
kafkaConsumerManager.init(configuration)
    .then(() => {})
```
##### Configuration

* `KAFKA_URL` &ndash; URL of Kafka.
* `GROUP_ID` &ndash; Defines the Consumer Group this process is consuming on behalf of.
* `KAFKA_CONNECTION_TIMEOUT` &ndash; Max wait time wait kafka to connect.
* `ZOOKEEPER_URL` &ndash; Zookeeper URL.
* `KAFKA_OFFSET_DIFF_THRESHOLD` &ndash; Tolerance for how far the partition offset of the consumer can be from the real offset, this value is used by the health check to reject in case the offset is out of sync.
* `TOPICS` &ndash; Array of topics that should be consumed.
* `RESUME_PAUSE_INTERVAL_MS` &ndash; Interval of when to run the RESUME_PAUSE_CHECK_FUNCTION.
* `RESUME_PAUSE_CHECK_FUNCTION` &ndash; Function that in case of return value is true, the consumer will be resumed, if false it will be paused.
* `MESSAGE_FUNCTION` &ndash; Function that applied to each consumed message, this function accepts one param (message).

### kafka-consumer-manager.init(configuration)

Init the consumer and the producer, make sure to pass full configuration object else you will get exceptions.

The function returns Promise.

### kafka-consumer-manager.healthCheck()

Runs a health check checking the connection to kafka, also testing that offset is not out of sync according to KAFKA_OFFSET_DIFF_THRESHOLD

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