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
        KafkaOffsetDiffThreshold: 3,
        Topics: ["TOPIC-A", "TOPIC-B"],
        ResumePauseIntervalMs: 30000,
        ResumePauseCheckFunction: () => {
            return shouldPauseConsuming()
        },
        MessageFunction: (msg) => { handleMessage(msg) },
        MaxMessagesInMemory: 100,
        ResumeMaxMessagesRatio: 0.25
        
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
* `KafkaOffsetDiffThreshold` &ndash; Tolerance for how far the partition offset of the consumer can be from the real offset, this value is used by the health check to reject in case the offset is out of sync.
* `Topics` &ndash; Array of topics that should be consumed.
* `ResumePauseIntervalMs` &ndash; Interval of when to run the ResumePauseCheckFunction (Optional).
* `ResumePauseCheckFunction` &ndash; Function that in case of return value is true, the consumer will be resumed, if false it will be paused (Optional).
* `MessageFunction` &ndash; Function that applied to each consumed message, this function accepts one param (message).
* `FetchMaxBytes` &ndash; The maximum bytes to include in the message set for this partition. This helps bound the size of the response. (Default 1024^2).
* `MaxMessagesInMemory` &ndash; If enabled, the consumer will pause after having this number of messages in memory, to lower the counter call the finishedHandlingMessage function (Optional).
* `ResumeMaxMessagesRatio` &ndash; If enabled when the consumer is paused it will resume only when MaxMessagesInMemory * ResumeMaxMessagesRatio < CurrentMessagesInMemory, number should be below 1 (Optional).

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

### kafka-consumer-manager.send(message, topic)

Send a message back to a topic.

### kafka-consumer-manager.finishedHandlingMessage()

Decrease the counter of how many messages currently processed in the service, used with combine of the env params: ResumeMaxMessagesRatio and MaxMessagesInMemory

Send a message back to a topic.

## Running Tests
Using mocha and istanbul 
```bash
npm test
```