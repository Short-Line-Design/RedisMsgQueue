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
var app = 'example', unqueue = null
  , queues = [ { name: 'first' }, { name: 'second' } ]
  , groups = [ { name: 'alpha' }, { name: 'bravo' }, { name: 'charlie' } ]

// Setup the ungrouped message queue
console.log('Setup the ungrouped message queue.');
redisMsgQueue.createRedisMsgQueue(app, 'unqueue', function (error, myQueue) {
    if (error) {
        console.warn('The example code was unable to create the ungrouped message queue - ' + util.inspect(error) + '.');
    }
    // Assign the returned queue to the example ungrouped message queue
    unqueue = { msgQueue: myQueue };

    // Register the message queue workers
    for (var i = 0; i < 4; i++) {
        unqueue.msgQueue.register(function (myTask, callback) {
            console.log('One of five ungrouped workers - processing task ' + util.inspect(myTask) + '.');

            // Assume that some work is being done at this time
            return setTimeout(function (myTask, callback) {
                console.log('One of five ungrouped workers - processing task ' + util.inspect(myTask) + '.');
                return callback();
            }, 2500, myTask, callback);

        }, function (error, worker) {
            if (error) {
                console.warn('The example code was unable to register the ungrouped message queue worker ' + i +
                             ' - ' + util.inspect(error) + '.');
            }
            console.log('Registered ungrouped message queue worker ' + i + '.');
        });
    }

    // Add a few tasks to the example ungrouped message queue
    console.log('Adding a few tasks to the ungrouped message queue.');
    for (var j = 0; j < 500; j++) {
        var task = { name:'un-task ' + j, number: j };
        unqueue.msgQueue.enqueue(undefined, task, function (error, myTask) {
            if (error) {
                console.warn('The example was unable to add the task ' + util.inspect(task) +
                             ' to the ungrouped message queue - ' + util.inspect(myTask) +
                             ', ' + util.inspect(error) + '.');
            }
        });
    }
});

// Setup the grouped message queues
console.log('Setup each grouped message queue.');
queues.forEach(function (queue) {
    queue.msgQueue = redisMsgQueue.createRedisMsgQueue(app, queue.name);

    // Register the message queue workers
    for (var i = 0; i < 3; i++) {
        queue.msgQueue.register(function (myTask, callback) {
            console.log('One of five grouped workers for ' + queue.name  + ' - processing task ' + util.inspect(myTask) + '.');

            // Assume that some work is being done at this time
            return setTimeout(function (myTask, callback) {
                console.log('One of five grouped workers for ' + queue.name  + ' - processing task ' + util.inspect(myTask) + '.');
                return callback();
            }, 500, myTask, callback);

        }, function (error, worker) {
            if (error) {
                console.warn('The example code was unable to register the grouped message queue worker ' + i +
                             ' - ' + util.inspect(error) + '.');
            }
            console.log('Registered ' + queue.name + ' grouped message queue worker ' + i + '.');
        });
    }

    // Add a few tasks to the each grouped message queue
    console.log('Adding a few tasks to the grouped message queue.');
    groups.forEach(function (group) {
        for (var j = 0; j < 250; j++) {
            var task = { name:'task ' + j, number: j, queue: queue.name, group: group };
            queue.msgQueue.enqueue(group, task, function (error, myTask) {
                if (error) {
                    console.warn('The example was unable to add the task ' + util.inspect(task) +
                                 ' to the grouped message queue - ' + util.inspect(myTask) +
                                 ', ' + util.inspect(error) + '.');
                }
            });
        }
    });
});
