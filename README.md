# kafka-consumer-manager

[![Build Status](https://travis-ci.org/enudler/kafka-consumer-manager.svg?branch=master)](https://travis-ci.org/enudler/kafka-consumer-manager)

[![NPM](https://nodei.co/npm/kafka-consumer-manager.png?downloads=true&downloadRank=true&stars=true)](https://nodei.co/npm/kafka-consumer-manager/)
[![NPM](https://nodei.co/npm-dl/kafka-consumer-manager.png?months=1)](https://nodei.co/npm/kafka-consumer-manager/)

This package is used to simplify the common use of kafka consumer:
* Supports multiple kafka-consumer-manager by instance creation
* Provides support for autoCommit: false and throttling by saving messages to queues and working messages by message per partition, (concurrency level equals to the partitions number)
* Provides an api for kafka consumer offset out of sync check by checking that the offset of the partition is synced to zookeeper
* Accepts a promise with the business logic each consumed message should go through
* Accepts a promise function with the business logic of when to pause and when to resume the consuming
* Provides an api for sending message back to the topic (usually for retries)


## Install
```bash
npm install --save kafka-consumer-manager
```

## API

### How to use

```js
let KafkaConsumerManager = require('kafka-consumer-manager');
```

```js
let configuration = {
        KafkaUrl: "localhost:9092",
        GroupId: "some-group-id",
        KafkaConnectionTimeout: 10000,
        KafkaOffsetDiffThreshold: 3,
        Topics: ["TOPIC-A", "TOPIC-B"],
        ResumePauseIntervalMs: 30000,
        ResumePauseCheckFunction: (consumer) => {
            return shouldPauseConsuming(consumer)
        },
        MessageFunction: (msg) => { return handleMessage(msg) },
        MaxMessagesInMemory: 100,
        ResumeMaxMessagesRatio: 0.25,
        CreateProducer: false
    };

```

```js   
(async () => {
let kafkaConsumerManager = new KafkaConsumerManager()
await kafkaConsumerManager.init(configuration)
    .then(() => {})
    })()
```
##### Configuration

* `KafkaUrl` &ndash; URL of Kafka.
* `GroupId` &ndash; Defines the Consumer Group this process is consuming on behalf of.
* `KafkaConnectionTimeout` &ndash; Max wait time wait kafka to connect.
* `KafkaOffsetDiffThreshold` &ndash; Tolerance for how far the partition offset of the consumer can be from the real offset, this value is used by the health check to reject in case the offset is out of sync.
* `Topics` &ndash; Array of topics that should be consumed.
* `ResumePauseIntervalMs` &ndash; Interval of when to run the ResumePauseCheckFunction (Optional).
* `ResumePauseCheckFunction` &ndash; Promise that should always be resolve. In case of resolve with true value, the consumer will be resumed, if false it will be paused (Mandatory if ResumePauseIntervalMs provided). this function accepts one param (consumer).
* `MessageFunction` &ndash; Promise that should always be resolve. this function applied to each consumed message, It accepts one param (message), please make sure to resolve only after messages is considered as done. Don't change the original message, it may cause it may cause unstable behaviour in getLastMessage function.
* `FetchMaxBytes` &ndash; The maximum bytes to include in the message set for this partition. This helps bound the size of the response. (Default 1024^2).
* `WriteBackDelay` &ndash; Delay the produced messages by ms. (optional).
* `AutoCommit` &ndash; Boolean, If AutoCommit is false, the consumer will queue messages from each partition to a specific queue and will handle messages by the order and commit the offset when it's done.
* `LoggerName` &ndash; String, the value of consumer_name field of the internal logger, if empty this field will not exist.
* `CreateProducer` &ndash; Boolean, If CreateProducer is true it will create Producer instance.(Default true)


##### AutoCommit: true settings

* `MaxMessagesInMemory` &ndash; If enabled, the consumer will pause after having this number of messages in memory, to lower the counter call the finishedHandlingMessage function (Optional).
* `ResumeMaxMessagesRatio` &ndash; If enabled when the consumer is paused it will resume only when MaxMessagesInMemory * ResumeMaxMessagesRatio < CurrentMessagesInMemory, number should be below 1 (Optional).

##### AutoCommit: false settings

* `ThrottlingThreshold` &ndash; If the consumer will have more messages than this value it will pause, it will resume consuming once the value is below that given threshold`.
* `ThrottlingCheckIntervalMs` &ndash; The interval in ms of when to check if messages are above or below the threshold`.
* `CommitEachMessage` &ndash; Boolean, If CommitEachMessage is false the commit will be each AutoCommitIntervalMs.(Default true)
* `AutoCommitIntervalMs` &ndash; The interval in ms to make commit to the broker, relevant only if CommitEachMessage is false.(Default 5000)

### await kafka-consumer-manager.init(configuration)

Init the consumer and the producer, make sure to pass full configuration object else you will get exceptions.

The function returns Promise.

### kafka-consumer-manager.validateOffsetsAreSynced()

Runs a check the offset of the partitions are synced and moving as expected by checking progress and zookeeper offsets.

The function returns Promise.

### await kafka-consumer-manager.closeConnection()

Closes the connection to kafka, return Promise.

### kafka-consumer-manager.pause()

Pause the consuming of new messages.

### kafka-consumer-manager.resume()

Resume the consuming of new messages.

### kafka-consumer-manager.send(message, topic)

Send a message back to a topic. returns a promise.

### kafka-consumer-manager.finishedHandlingMessage()

Decrease the counter of how many messages currently processed in the service, used with combine of the env params: ResumeMaxMessagesRatio and MaxMessagesInMemory
Only relevant for autoCommit: true

### kafka-consumer-manager.getLastMessage()

Get the last message that the consumer received. Don't change the original message, it may cause unstable behaviour in MessageFunction function.

### kafka-consumer-manager.on(eventName, eventHandler)

Listens on the chose consumer events

## Running Tests
Using mocha and istanbul 
```bash
npm test
```