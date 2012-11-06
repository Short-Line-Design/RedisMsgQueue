/*
 * The Redis Message Queue
 *
 * Copyright (c) 2012 Short Line Design Inc.
 * Copyright (c) 2012 Dan Prescott <danpres14@gmail.com>
 * The MIT License (MIT)
 *
 * The Redis Message Queue is a lightweight message queue
 * designed to provide sequential processing of all tasks in
 * a message group (based on a group key) while maintaining highly
 * parallel processing across multiple message groups with disparate
 * keys.  This message queue also provides for highly parallel processing
 * of all tasks that have not been assigned to a message group (the group key is
 * undefined) such that each task is passed to a single worker's callback function.
 *
 */

var _ = require('underscore')
  , redis = require('redis')
  , util = require('util')

  , worker = require('./worker')
  , queue = require('./queue')
  , task = require('./task');

// ----------------------------------------------------------------------------------------------------
// The msg queue static functions and variables
// ----------------------------------------------------------------------------------------------------
var _msgQueueMap = {}, _msgWorkerMap = {}; _msgWorkerCount = 0;

var _addMsgQueue = function (queueName, msgQueue, callback) {
    // Make sure the msg queue does not exist
    if (_.isUndefined(_msgQueueMap[queueName])) {
        // Add the queue (with wrapper) to the map
        _msgQueueMap[queueName] = {
            queueName: queueName,
            msgQueue: msgQueue
        };
        // Return success via return or callback
        if (_.isFunction(callback)) {
            return callback();
        }
        return true;
    }
    // Return existance via return or callback
    if (_.isFunction(callback)) {
        return callback('The msg queue ' + queueName + ' already exists.');
    }
    return false;
};

var _addMsgWorker = function (queueName, msgWorker, callback) {
    // Set the msg worker name (unique within this process)
    var workerName = '' + (++_msgWorkerCount) + '-' + queueName;
    // Add the worker (with wrapper) to the map
    _msgWorkerMap[workerName] = {
        workerName: workerName,
        msgWorker: msgWorker,
        stop: false
    };
    // Return success via return or callback
    if (_.isFunction(callback)) {
        return callback(undefined, workerName);
    }
    return workerName;
};

var _getMsgQueue = function (queueName, callback) {
    // Make sure the msg queue exists
    if (!_.isUndefined(_msgQueueMap[queueName])) {
        // Return the msg queue via return or callback
        if (_.isFunction(callback)) {
            return callback(undefined, _msgQueueMap[queueName].msgQueue);
        }
        return _msgQueueMap[queueName].msgQueue;
    }
    // Return non-existance via return or callback
    if (_.isFunction(callback)) {
        return callback('The msg queue ' + queueName + ' does not exist.');
    }
    return undefined;
};

var _getMsgWorker = function (workerName, callback) {
    // Make sure the msg worker exists
    if (!_.isUndefined(_msgWorkerMap[workerName])) {
        // Return the msg worker via return or callback
        if (_.isFunction(callback)) {
            return callback(undefined, _msgWorkerMap[workerName].msgWorker);
        }
        return _msgWorkerMap[workerName].msgWorker;
    }
    // Return non-existance via return or callback
    if (_.isFunction(callback)) {
        return callback('The msg worker ' + workerName + ' does not exist.');
    }
    return undefined;
};

var _getMsgWorkerStop = function (workerName, callback) {
    // Make sure the msg worker exists
    if (!_.isUndefined(_msgWorkerMap[workerName])) {
        // Return the msg worker stop via return or callback
        if (_.isFunction(callback)) {
            return callback(undefined, _msgWorkerMap[workerName].stop);
        }
        return _msgWorkerMap[workerName].stop;
    }
    // Return non-existance via return or callback
    if (_.isFunction(callback)) {
        return callback('The msg worker ' + workerName + ' does not exist.');
    }
    return undefined;
};

var _setMsgWorkerStop = function (workerName, callback) {
    // Make sure the msg worker exists
    if (!_.isUndefined(_msgWorkerMap[workerName])) {
        // Set the msg worker stop
        _msgWorkerMap[workerName].stop = true;
        // Return the msg worker stop via return or callback
        if (_.isFunction(callback)) {
            return callback(undefined, _msgWorkerMap[workerName].stop);
        }
        return _msgWorkerMap[workerName].stop;
    }
    // Return non-existance via return or callback
    if (_.isFunction(callback)) {
        return callback('The msg worker ' + workerName + ' does not exist.');
    }
    return undefined;
};

var _removeMsgQueue = function (queueName, callback) {
    // Make sure the msg queue exists
    if (!_.isUndefined(_msgQueueMap[queueName])) {
        // Remove the queue from the map
        delete _msgQueueMap[queueName];
        // Return success via return or callback
        if (_.isFunction(callback)) {
            return callback();
        }
        return true;
    }
    // Return non-existance via return or callback
    if (_.isFunction(callback)) {
        return callback('The msg queue ' + queueName + ' does not exist.');
    }
    return false;
};

var _removeMsgWorker = function (workerName, callback) {
    // Make sure the msg worker exists
    if (!_.isUndefined(_msgWorkerMap[workerName])) {
        // Remove the worker from the map
        delete _msgWorkerMap[workerName];
        // Return success via return or callback
        if (_.isFunction(callback)) {
            return callback();
        }
        return true;
    }
    // Return non-existance via return or callback
    if (_.isFunction(callback)) {
        return callback('The msg worker ' + workerName + ' does not exist.');
    }
    return false;
};

var _setMsgQueueName = function (appName, queueName) {
    // Combine the app name with the queue name
    var myMsgQueueName = (_.isString(appName) && !_.isEmpty(appName)) ? appName : 'RedisMsgQueue';
    if (_.isString(queueName) && !_.isEmpty(queueName)) {
        myMsgQueueName += ':' + queueName;
    } else {
        myMsgQueueName += ':--';
    }
    return myMsgQueueName;
};

var _setMsgGroupName = function (appName, queueName, groupName) {
    // Combine the app name and queue name with the group name
    var myMsgGroupName = _setMsgQueueName(appName, queueName);
    if (_.isString(groupName) && !_.isEmpty(groupName)) {
        myMsgGroupName += ':' + groupName;
    } else {
        myMsgGroupName += ':--';
    }
    return myMsgGroupName;
};

// ----------------------------------------------------------------------------------------------------
// The msg queue class factory methods and the msg queue class
// ----------------------------------------------------------------------------------------------------
var createRedisMsgQueue = function (appName, queueName, redisOptions, callback) {
    if (_.isFunction(redisOptions)) {
        callback = redisOptions;
        redisOptions = undefined;
    }
    var msgQueueName = _setMsgQueueName(appName, queueName);
    if (_.isFunction(callback)) {
        // Return an existing msg queue (if one exists)
        return _getMsgQueue(msgQueueName, function (error, msgQueue) {
            if (!error) {
                // Return the existing msg queue
                return callback(undefined, msgQueue);
            }
            // Create and return a new msg queue
            return callback(undefined, new RedisMsgQueue(appName, queueName, redisOptions));
        })
    }
    // Return an existing msg queue (if one exists)
    var msgQueue = _getMsgQueue(msgQueueName);
    if (msgQueue) {
        // Return the existing msg queue
        return msgQueue;
    }
    // Create and return a new msg queue
    return new RedisMsgQueue(appName, queueName, redisOptions);
};
module.exports.createRedisMsgQueue = createRedisMsgQueue;
module.exports.getRedisMsgQueue = createRedisMsgQueue;

var RedisMsgQueue = function (appName, queueName, redisOptions) {
    // Set the app and queue names for this instance of the queue
    this.appName = appName;
    this.queueName = queueName;
    this.msgQueueName = _setMsgQueueName(appName, queueName);

    // Set the poll interval, retry delay, and retry limit for this instance of the queue
    this.pollInterval = 15000;
    this.retryDelay = 1.25;
    this.retryLimit = 1;

    // Set the redis options for this instance of the queue
    this.options = {};
    if (_.isObject(redisOptions)) {
        this.options = redisOptions;
    }
    this.host = this.options.host;
    this.port = this.options.port;
    this.passwd = this.options.password;

    // Add the queue to the static msg queue map (to allow clean shutdown)
    if (!_addMsgQueue(this.msgQueueName, this)) {
        console.warn('RedisMsgQueue()  The redis msg queue constructor was unable to add the queue to the msg queue map.');
    }

    // Setup the redis client and prepare the connection
    this.redisClient = redis.createClient(this.port, this.host, this.options);
    if (this.passwd) {
        this.redisClient.auth(this.passwd, function (error, reply) {
            console.warn('RedisMsgQueue()  The redis client was unable to authenticate to the redis server - ' + util.inspect(error) + '.')
        });
    }
};

// ----------------------------------------------------------------------------------------------------
// The msg queue clear method
// ----------------------------------------------------------------------------------------------------
RedisMsgQueue.prototype.clear = function (group, callback) {
    return callback('The redis msg queue has not yet implemented the clear method.');
};

// ----------------------------------------------------------------------------------------------------
// The msg queue enqueue method
// ----------------------------------------------------------------------------------------------------
RedisMsgQueue.prototype.enqueue = function (group, task, callback) {
    // Set the msg group name and the task string to push onto the queue
    var appName = this.appName, queueName = this.queueName, msgQueueName = this.msgQueueName;
    var msgGroupName = _setMsgGroupName(appName, queueName, group);
    var taskJsonString = JSON.stringify(task);

    // Push the task (in json string format) onto the left end of the queue
    var multi = this.redisClient.multi();
    // Push the task onto the left end of the app:queue:group list
    multi.lpush('list_' + msgGroupName, taskJsonString);
    // Increment the number of tasks on the app:queue:group hash
    multi.hincrby('hash_' + msgGroupName, 'tasks', 1);
    // Add the app:queue:group to the app:queue set - this will only
    // add the app:queue:group if it does not already exist in the set
    multi.sadd('set_' + msgQueueName, msgGroupName);
    return multi.exec(function (error, replies) {
        if (error) {
            console.warn('RedisMsgQueue.enqueue()  The redis msg queue ' + msgQueueName +
                         ' was unable to add the task for the message group ' + msgGroupName +
                         ' - error ' + util.inspect(error) + '.');
            return callback('The redis msg queue ' + queueName +
                            ' was unable to add the task for the message group ' + group + '.');
        }
        // Make sure each step was successful
        if (!_.isArray(replies) || replies.length !== 3) {
            console.warn('RedisMsgQueue.enqueue()  The redis msg queue ' + msgQueueName +
                         ' was unable to add the task for the message group ' + msgGroupName +
                         ' - the status of one or more steps was not reported, replies ' + util.inspect(replies) + '.');
            return callback('The redis msg queue ' + queueName +
                            ' was unable to add the task for the message group ' + group + '.');
        }
        if (!_.isFinite(replies[0]) || replies[0] === 0) {
            console.warn('RedisMsgQueue.enqueue()  The redis msg queue ' + msgQueueName +
                         ' was unable to add the task for the message group ' + msgGroupName +
                         ' - the list reported a size of zero after pushing the task onto the list, reply ' + util.inspect(replies[0]) + '.');
            return callback('The redis msg queue ' + queueName +
                            ' was unable to add the task for the message group ' + group + '.');
        }
        if (!_.isFinite(replies[1]) || replies[1] <= 0) {
            console.warn('RedisMsgQueue.enqueue()  The redis msg queue ' + msgQueueName +
                         ' was unable to add the task for the message group ' + msgGroupName +
                         ' - the hash reported a zero or negative number of tasks after adding this task to the number, reply ' + util.inspect(replies[1]) + '.');
            return callback('The redis msg queue ' + queueName +
                            ' was unable to add the task for the message group ' + group + '.');
        }
        if (!_.isFinite(replies[2]) || (replies[2] !== 0 && replies[2] !== 1)) {
            console.warn('RedisMsgQueue.enqueue()  The redis msg queue ' + msgQueueName +
                         ' was unable to add the task for the message group ' + msgGroupName +
                         ' - the set reported adding more or less than zero or one message group to the message groups, reply ' + util.inspect(replies[2]) + '.');
            return callback('The redis msg queue ' + queueName +
                            ' was unable to add the task for the message group ' + group + '.');
        }
        // Return success via callback
        return callback(null, task);
    });
};

// ----------------------------------------------------------------------------------------------------
// The msg queue register worker method
// ----------------------------------------------------------------------------------------------------
RedisMsgQueue.prototype.register = function (worker, callback) {
    // Add the worker to the static worker map (to allow clean shutdown)
    var workerName = _addMsgWorker(this.queueName, worker);
    if (!_.isString(workerName) || _.isEmpty(workerName)) {
        console.warn('RedisMsgQueue()  The redis msg queue ' + this.msgQueueName + ' was unable to register the worker ' +
                     util.inspect(workerName) + ' with the msg worker map.');
        return callback('The redis msg queue ' + this.queueName + ' was unable to register the worker.');
    }

    // Return success via callback - do not
    // return execution along with the callback!
    callback();

    // Setup a function to stop each worker
    // when the process is about to exit
    var msgQueue = this;
    process.on('exit', function (error, callback) {
        // Tell the worker to exit
// TODO - Fix This!  I need to add something to gracefully shutdown after all workers really close
        return _setMsgWorkerStop(workerName);
    });

    // Setup a new redis client and prepare the connection - an individual client
    // is required for each worker so that the watch mechanism will function properly
    var workerRedis = redis.createClient(this.port, this.host, this.options);
    if (this.passwd) {
        this.redisClient.auth(this.passwd, function (error, reply) {
            console.warn('RedisMsgQueue()  The worker redis client was unable to authenticate to the redis server - ' + util.inspect(error) + '.')
        });
    }

    // Start each worker (as a service) allowing
    // each worker to run until the process exits.
    return this._timeoutProcess(workerName, workerRedis, worker, this, this.pollInterval);
};

// ----------------------------------------------------------------------------------------------------
// The msg queue timed task processing (private)
// ----------------------------------------------------------------------------------------------------
RedisMsgQueue.prototype._timeoutProcess = function (workerName, workerRedis, msgWorker, msgQueue, timeout) {
    return setTimeout(function () {
        return msgQueue._process(workerName, workerRedis, msgWorker, msgQueue);
    }, timeout);
};

// ----------------------------------------------------------------------------------------------------
// The msg queue task processing (private)
// ----------------------------------------------------------------------------------------------------
RedisMsgQueue.prototype._process = function (workerName, workerRedis, msgWorker, msgQueue) {
    // Exit the processing loop if the worker has been told to stop
    if (_getMsgWorkerStop(workerName)) {
        if (!msgQueue._removeMsgWorker(workerName)) {
            console.warn('RedisMsgQueue._process()  The redis msg worker ' + msgWorkerName +
                         ' was unable to remove itself from the worker map.');
        }
        return 'This worker is done.';
    }

    // Pull a random member (app:queue:group) from the app:queue set
    return workerRedis.srandmember('set_' + msgQueue.msgQueueName, function (error, reply) {
        if (error) {
            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                         ' was unable to pull a random member (app:queue:group) from the set ' +
                         ' - reply ' + util.inspect(reply) + ', error ' + util.inspect(error) + '.');
            return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, msgQueue.pollInterval);
        }
        if (!_.isString(reply) || _.isEmpty(reply)) {
            return workerRedis.watch('set_' + msgQueue.msgQueueName, function(error, reply) {
                if (error || reply !== 'OK') {
                    console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                 ' was unable to watch the app:queue set ' + msgQueue.msgQueueName + ' - reply ' +
                                 util.inspect(reply) + ', error ' + util.inspect(error) + '.');
                    if (!error) {
                        return workerRedis.unwatch(function (error, reply) {
                            if (error || reply !== 'OK') {
                                console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                             ' was unable to unwatch the app:queue set ' + msgQueue.msgQueueName +
                                             ' - reply ' + util.inspect(reply) + ', error ' + util.inspect(error) + '.');
                            }
                            return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, msgQueue.pollInterval);
                        });
                    }
                    return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, msgQueue.pollInterval);
                }

                // Read the number of app:queue:group items in the app:queue set
                return workerRedis.scard('set_' + msgQueue.msgQueueName, function (error, reply) {
                    if (error || !_.isFinite(reply)) {
                        console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName + 'was unable to read the' +
                                     ' number of app:queue:group items in the app:queue set ' + msgQueue.msgQueueName +
                                     ' - reply ' + util.inspect(reply) + ', error ' + util.inspect(error) + '.');
                        return workerRedis.unwatch(function (error, reply) {
                            if (error || reply !== 'OK') {
                                console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                             ' was unable to unwatch the app:queue set ' + msgQueue.msgQueueName +
                                             ' - reply ' + util.inspect(reply) + ', error ' + util.inspect(error) + '.');
                            }
                            return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, msgQueue.pollInterval);
                        });
                    }

                    // Remove the app:queue set if the number of app:queue:group items is zero
                    if (reply === 0) {
                        // Remove the app:queue set
                        var multi = workerRedis.multi();
                        multi.del('set_' + msgQueue.msgQueueName);
                        return multi.exec(function (error, replies) {
                            if (error) {
                                console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName + 'was unable to remove the' +
                                             ' app:queue set ' + msgQueue.msgQueueName + ' - reply ' + util.inspect(replies) + ', error ' + util.inspect(error) + '.');
                                return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, msgQueue.pollInterval);
                            }
                            // Check for non-execution due to watched key changing
                            if (_.isNull(replies)) {
                                return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, 0);
                            }
                            if (!_.isArray(replies) || replies.length !== 1) {
                                console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName + 'was unable to remove the' +
                                             ' app:queue set ' + msgQueue.msgQueueName + ' - reply ' + util.inspect(replies) + '.');
                                return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, msgQueue.pollInterval);
                            }
                            var reply = replies[0];
                            if (_.isString(reply) && !_.isEmpty(reply) && _.isFinite(parseInt(reply))) {
                                reply = parseInt(reply);
                            }
                            if (!_.isFinite(reply) || (reply !== 0 && reply !== 1)) {
                                console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName + 'was unable to remove the' +
                                             ' app:queue set ' + msgQueue.msgQueueName + ' - replies ' + util.inspect(replies) + '.');
                            }
                            return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, msgQueue.pollInterval);
                        });
                    }
                    return workerRedis.unwatch(function (error, reply) {
                        if (error || reply !== 'OK') {
                            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                         ' was unable to unwatch the app:queue set ' + msgQueue.msgQueueName +
                                         ' - reply ' + util.inspect(reply) + ', error ' + util.inspect(error) + '.');
                        }
                        return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, msgQueue.pollInterval);
                    });
                });
            });
        }

        // Setup the watch for the app:queue:group key - this is the 'reply' from the last query
        var randAppQueueGroup = reply;
        return workerRedis.watch('hash_' + randAppQueueGroup, function(error, reply) {
            if (error || reply !== 'OK') {
                console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                             ' was unable to watch the app:queue:group hash ' + randAppQueueGroup + ' - reply ' +
                             util.inspect(reply) + ', error ' + util.inspect(error) + '.');
                if (!error) {
                    return workerRedis.unwatch(function (error, reply) {
                        if (error || reply !== 'OK') {
                            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                         ' was unable to unwatch the app:queue:group hash ' + randAppQueueGroup +
                                         ' - reply ' + util.inspect(reply) + ', error ' + util.inspect(error) + '.');
                        }
                        return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, msgQueue.pollInterval);
                    });
                }
                return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, msgQueue.pollInterval);
            }

            // Read the number of tasks and the locked datetime in the app:queue:group
            return workerRedis.hmget('hash_' + randAppQueueGroup, 'tasks', 'locked', function (error, replies) {
                if (error || !_.isArray(replies) || replies.length !== 2) {
                    console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName + ' was unable to read the' +
                                 ' number of tasks and the locked datetime in the app:queue:group hash ' + randAppQueueGroup +
                                 ' - replies ' + util.inspect(replies) + ', error ' + util.inspect(error) + '.');
                    return workerRedis.unwatch(function (error, reply) {
                        if (error || reply !== 'OK') {
                            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                         ' was unable to unwatch the app:queue:group hash ' + randAppQueueGroup +
                                         ' - reply ' + util.inspect(reply) + ', error ' + util.inspect(error) + '.');
                        }
                        return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, msgQueue.pollInterval);
                    });
                }

                // Run the process loop again if the number of tasks is zero or if the locked datetime is set
                var tasks = null;
                if (_.isNull(replies[0])) {
                } else if (_.isFinite(replies[0])) {
                    tasks = replies[0];
                } else if (_.isString(replies[0]) && !_.isEmpty(replies[0]) && _.isFinite(parseInt(replies[0]))) {
                    tasks = parseInt(replies[0]);
                } else {
                    console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName + ' was unable to read the' +
                                 ' number of tasks in the app:queue:group hash ' + randAppQueueGroup + ' - reply ' + util.inspect(replies[0]) +
                                 ', error ' + util.inspect(error) + '.');
                    return workerRedis.unwatch(function (error, reply) {
                        if (error || reply !== 'OK') {
                            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                         ' was unable to unwatch the app:queue:group hash ' + randAppQueueGroup +
                                         ' - reply ' + util.inspect(reply) + ', error ' + util.inspect(error) + '.');
                        }
                        return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, msgQueue.pollInterval);
                    });
                }
                var locked = null;
                if (_.isNull(replies[1])) {
                } else if (_.isDate(replies[1])) {
                    locked = replies[1];
                } else if (_.isString(replies[1]) && !_.isEmpty(replies[1]) && _.isDate(Date.parse(replies[1]))) {
                    locked = Date.parse(replies[1]);
                } else {
                    console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName + ' was unable to read the' +
                                 ' locked datetime in the app:queue:group hash ' + randAppQueueGroup + ' - reply ' + util.inspect(replies[1]) +
                                 ', error ' + util.inspect(error) + '.');
                    return workerRedis.unwatch(function (error, reply) {
                        if (error || reply !== 'OK') {
                            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                         ' was unable to unwatch the app:queue:group hash ' + randAppQueueGroup +
                                         ' - reply ' + util.inspect(reply) + ', error ' + util.inspect(error) + '.');
                        }
                        return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, msgQueue.pollInterval);
                    });
                }
                if (_.isDate(locked)) {
                    //console.log('Worker ' + workerName + ' - test point - locked');
                    return workerRedis.unwatch(function (error, reply) {
                        if (error || reply !== 'OK') {
                            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                         ' was unable to unwatch the app:queue:group hash ' + randAppQueueGroup +
                                         ' - reply ' + util.inspect(reply) + ', error ' + util.inspect(error) + '.');
                        }
                        return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, 1);
                    });
                }
                if (tasks === 0) {
                    //console.log('Worker ' + workerName + ' - test point - clean up tasks === 0');
                    // Cleanup the data store if there are no more tasks in the app:queue:group
                    var multi = workerRedis.multi();
                    // Delete the app:queue:group hash
                    multi.del('hash_' + randAppQueueGroup);
                    // Delete the app:queue:group list
                    multi.del('list_' + randAppQueueGroup);
                    // Remove the app:queue:group from the set
                    multi.srem('set_' + msgQueue.msgQueueName, randAppQueueGroup);
                    return multi.exec(function (error, replies) {
                        if (error) {
                            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                         ' was unable to cleanup the data store for the msg group ' + randAppQueueGroup +
                                         ' - error ' + util.inspect(error) + '.');
                            return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, msgQueue.pollInterval);
                        }
                        // Check for non-execution due to watched key changing
                        if (_.isNull(replies)) {
                            return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, 1);
                        }
                        // Make sure each step was successful
                        if (!_.isArray(replies) || replies.length !== 3) {
                            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                         ' was unable to cleanup the data store for the msg group ' + randAppQueueGroup +
                                         ' - the status of one or more steps was not reported, replies ' + util.inspect(replies) + '.');
                            return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, msgQueue.pollInterval);
                        }
                        var reply = replies[0];
                        if (_.isString(reply) && !_.isEmpty(reply) && _.isFinite(parseInt(reply))) {
                            reply = parseInt(reply);
                        }
                        if (!_.isFinite(reply) || (reply !== 0 && reply !== 1)) {
                            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                         ' was unable to cleanup the data store for the msg group ' + randAppQueueGroup +
                                         ' - the delete app:queue:group hash indicated more or less than one delete, reply ' + util.inspect(replies[0]) + '.');
                        }
                        reply = replies[1];
                        if (_.isString(reply) && !_.isEmpty(reply) && _.isFinite(parseInt(reply))) {
                            reply = parseInt(reply);
                        }
                        if (!_.isFinite(reply) || (reply !== 0 && reply !== 1)) {
                            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                         ' was unable to cleanup the data store for the msg group ' + randAppQueueGroup +
                                         ' - the delete app:queue:group list indicated more or less than one delete, reply ' + util.inspect(replies[1]) + '.');
                        }
                        reply = replies[2];
                        if (_.isString(reply) && !_.isEmpty(reply) && _.isFinite(parseInt(reply))) {
                            reply = parseInt(reply);
                        }
                        if (!_.isFinite(reply) || (reply !== 0 && reply !== 1)) {
                            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                         ' was unable to cleanup the data store for the msg group ' + randAppQueueGroup +
                                         ' - the remove app:queue:group from the set indicated more or less than one remove, reply ' + util.inspect(replies[2]) + '.');
                        }
                        return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, msgQueue.pollInterval);
                    });
                }

                //console.log('Worker ' + workerName + ' - test point - just before read and pop task');
                // Read and pop the task (in json string format) from the right end of the queue
                var stepCount = 2; // First two steps - rpop and hincrby
                var multi = workerRedis.multi();
                // Read and pop the task from the right end of the app:queue:group list
                multi.rpop('list_' + randAppQueueGroup);
                // Decrement the number of tasks on the app:queue:group hash
                multi.hincrby('hash_' + randAppQueueGroup, 'tasks', -1);
                // Set the locked datetime only if the group was specified with the task
                if (randAppQueueGroup !== _setMsgGroupName(msgQueue.appName, msgQueue.queueName))
                {
                    stepCount += 1; // Lock step (if locked) - hset
                    multi.hset('hash_' + randAppQueueGroup, 'locked', new Date().toISOString());
                }
                return multi.exec(function (error, replies) {
                    if (error) {
                        console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                     ' was unable to read a task from the msg group ' + randAppQueueGroup +
                                     ' - error ' + util.inspect(error) + '.');
                        return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, msgQueue.pollInterval);
                    }

                    // Check for non-execution due to watched key changing
                    if (_.isNull(replies)) {
                        //console.log('Worker ' + workerName + ' - test point - multi returned null multi-bulk due to watch');
                        return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, 1);
                    }
                    // Make sure each step was successful
                    if (!_.isArray(replies) || replies.length !== stepCount) {
                        console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                     ' was unable to read a task from the msg group ' + randAppQueueGroup +
                                     ' - the status of one or more steps was not reported, replies ' + util.inspect(replies) + '.');
                        return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, msgQueue.pollInterval);
                    }
                    if (!_.isObject(replies[0]) && (!_.isString(replies[0]) || _.isEmpty(replies[0]))) {
                        console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                     ' was unable to read a task from the msg group ' + randAppQueueGroup +
                                     ' - the list returned an invalid task, reply ' + util.inspect(replies[0]) + '.');
                        return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, 1);
                    }
                    var reply = replies[1];
                    if (_.isString(reply) && !_.isEmpty(reply) && _.isFinite(parseInt(reply))) {
                        reply = parseInt(reply);
                    }
                    if (!_.isFinite(reply) || reply < 0) {
                        console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                     ' was unable to read a task from the msg group ' + randAppQueueGroup +
                                     ' - the list returned an invalid number of tasks, reply ' + util.inspect(replies[1]) + '.');
                    }
                    if (randAppQueueGroup !== _setMsgGroupName(msgQueue.appName, msgQueue.queueName)) {
                        reply = replies[2];
                        if (_.isString(reply) && !_.isEmpty(reply) && _.isFinite(parseInt(reply))) {
                            reply = parseInt(reply);
                        }
                        if (!_.isFinite(reply) || (reply !== 0 && reply !== 1)) {
                            console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                         ' was unable to read a task from the msg group ' + randAppQueueGroup +
                                         ' - the hash key \'locked\' indicated that it was not set, reply ' + util.inspect(replies[2]) + '.');
                        }
                    }

                    //console.log('Worker ' + workerName + ' - test point - just before execute worker');
                    // Execute the worker with the task
                    var task = _.isObject(replies[0]) ? replies[0] : JSON.parse(replies[0]);
                    return msgWorker(task, function(error, myTask) {
                        if (randAppQueueGroup !== _setMsgGroupName(msgQueue.appName, msgQueue.queueName)) {
                            return workerRedis.hdel('hash_' + randAppQueueGroup, 'locked', function (error, reply) {
                                if (error) {
                                    console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                                 ' was unable to read a task from the msg group ' + randAppQueueGroup +
                                                 ' - the hash key \'locked\' was not deleted, error ' + util.inspect(error) + '.');
                                }
                                if (!_.isFinite(reply) || (reply !== 0 && reply !== 1)) {
                                    console.warn('RedisMsgQueue._process()  The redis msg queue ' + msgQueue.msgQueueName +
                                                 ' was unable to read a task from the msg group ' + randAppQueueGroup +
                                                 ' - the hash key \'locked\' indicated more or less than one remove, reply ' + util.inspect(reply) + '.');
                                }
                                return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, 1);
                            });
                        }
                        return msgQueue._timeoutProcess(workerName, workerRedis, msgWorker, msgQueue, 1);
                    });
                });
            });
        });
    });
};
