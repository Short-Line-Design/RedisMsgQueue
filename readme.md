# RedisMsgQueue

The Redis Message Queue is a lightweight message queue
designed to provide sequential processing of all tasks in
a message group (based on a group key) while maintaining highly
parallel processing across multiple message groups with disparate
keys.  This message queue also provides for highly parallel processing
of all tasks that have not been assigned to a message group (the group key is
undefined) such that each task is passed to a single worker's callback function.


### Installation

    $ npm install redis-msg-queue


### API


### Tests


### License

The MIT License (MIT)

Copyright (c) 2012 Short Line Design Inc.  
Copyright (c) 2012 Dan Prescott <danpres14@gmail.com>  

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
