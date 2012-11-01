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

