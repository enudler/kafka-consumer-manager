'use strict';

let bunyan = require('bunyan'),
    LOG_LEVEL = process.env.LOG_LEVEL || 'info',
    logger = bunyan.createLogger({
        name: 'kafka-consumer-manager',
        level: LOG_LEVEL
    });

module.exports = logger;