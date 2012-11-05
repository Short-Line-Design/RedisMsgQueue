/*
 * The Redis Message Queue - node.js example application
 *
 * Copyright (c) 2012 Short Line Design Inc.
 * Copyright (c) 2012 Dan Prescott <danpres14@gmail.com>
 * The MIT License (MIT)
 *
 */

var _ = require('underscore'),
    util = require('util'),

    redisMsgQueue = require('../index');

// Setup the example app, queues, and groups
var app = 'RedisMsgQueue-Example'
  , queues = [ { name: 'queue-1' }, { name: 'queue-2' } ]
  , groups = [ { name: 'group-a' }, { name: 'group-b' }, { name: 'group-c' }, { name: 'group-d' } ]

// Setup the message queues
console.log('Setup the first message queue.');

msgQueue = redisMsgQueue.createRedisMsgQueue(app, queues[0].name);

// Register the message queue workers
msgQueue.register(function(error, task, callback) {
    console.log('Processing task - ' + util.inspect(task));
    callback();
}, function (error, task) {
    console.log('Registered the worker.');
});

// Add the time series to the upstream aggregation message queue
msgQueue.enqueue(undefined, { task: 'example', description: 'This is an example' }, function(error, reply) {
    if (error) {
        console.warn('The example task was not queued - error ' + util.inspect(error) + '.');
    }
    console.warn('The example task was successfully queued.');
});
