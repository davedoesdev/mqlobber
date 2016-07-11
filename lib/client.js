/*
# mqlobber&nbsp;&nbsp;&nbsp;[![Build Status](https://travis-ci.org/davedoesdev/mqlobber.png)](https://travis-ci.org/davedoesdev/mqlobber) [![Coverage Status](https://coveralls.io/repos/davedoesdev/mqlobber/badge.png?branch=master&service=github)](https://coveralls.io/r/davedoesdev/mqlobber?branch=master) [![NPM version](https://badge.fury.io/js/mqlobber.png)](http://badge.fury.io/js/mqlobber)

Streaming message queue with pub-sub, work queues, wildcards and back-pressure.
Just Node and a filesystem required.

`mqlobber` basically remotes 
[`qlobber-fsq`](https://github.com/davedoesdev/qlobber-fsq) over one or more
connections.

<p align="center"><img src="http://rawgit.davedoesdev.com/davedoesdev/mqlobber/master/diagrams/overview.svg" width="80%"/></p>

Say you have a server and a number of clients, with the clients
connected to the server using some mechanism which provides a stream for each
connection. Create a `QlobberFSQ` instance on the server and for each stream,
pass the instance and the stream to `MQlobberServer`.

On each client, pass the other end of the stream to `MQlobberClient`. Clients
can then publish and subscribe to topics (including wildcard subscriptions).
Work queues are also supported - when publishing a message, a client can specify
that only one subscriber should receive it.

All data is transferred on streams multiplexed over each connection using
[`bpmux`](https://github.com/davedoesdev/bpmux), with full back-pressure support
on each stream. Clients get a `Writable` when publishing a message and a
`Readable` when receiving one.

You can scale out horizontally by creating a number of `QlobberFSQ` instances
(e.g. one per CPU core), all sharing the same message directory.

<p align="center"><img src="http://rawgit.davedoesdev.com/davedoesdev/mqlobber/master/diagrams/multi_instance.svg" width="80%"/></p>

No other backend services are required - just Node and a filesystem.

The API is described [here](#api).

## Example

First, let's create a server program which listens on a TCP port specified on
the command line:

```javascript
// server.js
var net = require('net'),
    QlobberFSQ = require('qlobber-fsq').QlobberFSQ,
    MQlobberServer = require('mqlobber').MQlobberServer,
    fsq = new QlobberFSQ();

fsq.on('start', function ()
{
    var server = net.createServer().listen(parseInt(process.argv[2]));
    server.on('connection', function (c)
    {
        new MQlobberServer(fsq, c);
    });
});
```

Next, a program which connects to the server and subscribes to messages
published to a topic:

```javascript
// client_subscribe.js
var assert = require('assert'),
    MQlobberClient = require('mqlobber').MQlobberClient,
    c = require('net').createConnection(parseInt(process.argv[2])),
    mq = new MQlobberClient(c),
    topic = process.argv[3];

mq.subscribe(topic, function (s, info)
{
    var msg = '';
    s.on('readable', function ()
    {
        var data;
        while ((data = this.read()) !== null)
        {
            msg += data.toString();
        }
    });
    s.on('end', function ()
    {
        console.log('received', info.topic, msg);
        assert.equal(msg, 'hello');
        c.end();
    });
});
```

Finally, a program which connects to the server and publishes a message to a
topic:

```javascript
// client_publish.js
var MQlobberClient = require('mqlobber').MQlobberClient,
    c = require('net').createConnection(parseInt(process.argv[2])),
    mq = new MQlobberClient(c);

mq.publish(process.argv[3], function ()
{
    c.end();
}).end('hello');
```

Run two servers listening on ports 8600 and 8601:

```shell
node server.js 8600 &
node server.js 8601 &
```

Subscribe to two topics, `foo.bar` and wildcard topic `foo.*`, one against each
server:

```shell
node client_subscribe.js 8600 foo.bar &
node client_subscribe.js 8601 'foo.*' &
```

Then publish a message to the topic `foo.bar`:

```shell
node client_publish.js 8600 foo.bar
```

You should see the following output, one line from each subscriber:

```
received foo.bar hello
received foo.bar hello
```

Only the servers should still be running and you can now terminate them:

```shell
$ jobs
[1]-  Running                 node server.js 8600 &
[2]+  Running                 node server.js 8601 &
$ kill %1 %2
[1]-  Terminated              node server.js 8600
[2]+  Terminated              node server.js 8601
```

## Installation

```shell
npm install mqlobber
```

## Licence

[MIT](LICENCE)

## Test

```shell
grunt test
```

## Lint

```shell
grunt lint
```

## Code Coverage

```shell
grunt coverage
```

[Instanbul](http://gotwarlost.github.io/istanbul/) results are available [here](http://rawgit.davedoes.com/davedoesdev/mqlobber/master/coverage/lcov-report/index.html).

Coveralls page is [here](https://coveralls.io/r/davedoesdev/mqlobber).

#API
*/

"use strict";

var EventEmitter = require('events').EventEmitter,
    BPMux = require('bpmux').BPMux,
    util = require('util'),
    QlobberDedup = require('qlobber').QlobberDedup,
    TYPE_SUBSCRIBE = 0,
    TYPE_UNSUBSCRIBE = 1,
    TYPE_UNSUBSCRIBE_ALL = 2,
    TYPE_PUBLISH = 3;

/**
Create a new `MQlobberClient` object for publishing and subscribing to
messages via a server.

@constructor

@param {Duplex} stream Connection to a server. The server should use
[`MQlobberServer`](#mqlobberserverfsq-stream-options) on its side of the
connection. How the connection is made is up to the caller - it just has to
supply a [`Duplex`](https://nodejs.org/dist/latest-v4.x/docs/api/stream.html#stream_class_stream_duplex). For example, [`net.Socket`](https://nodejs.org/dist/latest-v4.x/docs/api/net.html#net_class_net_socket) or [`PrimusDuplex`](https://github.com/davedoesdev/primus-backpressure#primusduplexmsg_stream-options).

@param {Object} [options] Configuration options. This is passed down to
[`QlobberDedup`](https://github.com/davedoesdev/qlobber#qlobberdedupoptions)
(which matches messages received from the server to handlers) and
[`BPMux`](https://github.com/davedoesdev/bpmux#bpmuxcarrrier-options)
(which multiplexes message streams over the connection to the
server). It also supports the following additional property:

  - `{Buffer} [handshake_data]` Application-specific handshake data to send to
    the server. The server-side [`MQlobberServer`](#mqlobberserverfsq-stream-options) object will
    emit this as a [`handshake`](#mqlobberservereventshandshakehandshake_data-delay_handshake) event to its
    application.
*/
function MQlobberClient(stream, options)
{
    EventEmitter.call(this);

    options = options || {};

    this._subs = new Map();
    this._matcher = new QlobberDedup(options);
    this.mux = new BPMux(stream, options);
    this._done = false;

    var ths = this;

    function error(err)
    {
        /*jshint validthis: true */
        ths.emit('error', err, this);
    }

    function warning(err)
    {
        /*jshint validthis: true */
        if (!ths.emit('warning', err, this))
        {
            console.error(err);
        }
    }
    this._warning = warning;

    this._unexpected_data = function ()
    {
        while (true)
        {
            if (this.read() === null)
            {
                break;
            }

            warning.call(this, new Error('unexpected data'));
        }
    };

    this.mux.on('error', error);

    this.mux.on('handshake_sent', function (duplex, complete)
    {
        if (!complete)
        {
            ths.emit('backoff');
        }
    });

    this.mux.on('finish', function ()
    {
        ths._done = true;
    });

    this.mux.on('end', function ()
    {
        ths._subs.clear();
    });

    var duplex = this.mux.multiplex({ handshake_data: options.handshake_data });
    duplex.on('error', warning);
    duplex.end(); // currently no data on initial duplex

    ths.mux.on('handshake', function (dplex, hdata, delay)
    {
        if (dplex === duplex)
        {
            return ths.emit('handshake', hdata);
        }

        if (!delay)
        {
            // duplex was initiated by us (outgoing message)
            return;
        }

        dplex.on('error', warning);
        dplex.end(); // only read from incoming message duplex

        if (hdata.length < 1)
        {
            return warning.call(dplex, new Error('buffer too small'));
        }

        var info = {},
            flags = hdata.readUInt8(0, true),
            pos = 1,
            topic;

        info.single = !!(flags & 1);

        if (flags & 2)
        {
            pos += 8;

            if (hdata.length < pos)
            {
                return warning.call(dplex, new Error('buffer too small'));
            }

            info.expires = hdata.readUInt32BE(1, true) * Math.pow(2, 32) + 
                           hdata.readUInt32BE(5, true);
        }

        info.topic = hdata.toString('utf8', pos); 

        for (var handler of ths._matcher.match(info.topic))
        {
            handler(dplex, info);
            if (info.single)
            {
                break;
            }
        }
    });
}

util.inherits(MQlobberClient, EventEmitter);

/**
Subscribe to messages published to the server.

@param {String} topic Which messages you're interested in receiving. Message
topics are split into words using `.` as the separator. You can use `*` to match
exactly one word in a topic or `#` to match zero or more words. For example,
`foo.*` would match `foo.bar` whereas `foo.#` would match `foo`, `foo.bar` and
`foo.bar.wup`. Note these are the default separator and wildcard characters.
They can be changed on the server when [constructing the `QlobberFSQ` object]
(https://github.com/davedoesdev/qlobber-fsq#qlobberfsqoptions) passed to
[`MQlobberServer`](#mqlobberserverfsq-stream-options).

@param {Function} handler Function to call when a new message is received from
the server due to its topic matching against `topic`. `handler` will be passed
the following arguments:

  - `{Readable} stream` The message content as a [Readable](http://nodejs.org/api/stream.html#stream_class_stream_readable). Note that _all_ subscribers will
    receive the same stream for each message.

  - `{Object} info` Metadata for the message, with the following properties:

    - `{String} topic` Topic to which the message was published.
    - `{Boolean} single` Whether this message is being given to _at most_ one 
      handler (across all clients connected to all servers).
    - `{Integer} expires` When the message expires (number of seconds after
      1 January 1970 00:00:00 UTC). This is only present if the server's
      [`MQlobberServer`](#mqlobberserverfsq-stream-options) instance is
      configured with `send_expires` set to `true`.

@param {Function} [cb] Optional function to call once the subscription has been
registered with the server. This will be passed the following argument:

  - `{Object} err` If an error occurred then details of the error, otherwise `null`.
*/
MQlobberClient.prototype.subscribe = function (topic, handler, cb)
{
    if (this._done)
    {
        throw new Error('finished');
    }

    var ths = this;

    function done()
    {
        var handlers = ths._subs.get(topic);

        if (!handlers)
        {
            handlers = new Set();
            ths._subs.set(topic, handlers);
        }

        handlers.add(handler);
        ths._matcher.add(topic, handler);

        if (cb)
        {
            cb();
        }
    }

    if (this._subs.has(topic))
    {
        return done();
    }

    var duplex = this.mux.multiplex(
    {
        handshake_data: Buffer.concat([new Buffer([TYPE_SUBSCRIBE]),
                                       new Buffer(topic, 'utf8')])
    });

    duplex.on('error', this._warning);
    duplex.on('readable', this._unexpected_data);
    duplex.end();

    if (cb)
    {
        return duplex.on('handshake', function (hdata)
        {
            if (hdata.length < 1)
            {
                return cb(new Error('buffer too small'));
            }

            if (hdata.readUInt8(0, true) !== 0)
            {
                return cb(new Error('server error'));
            }

            done();
        });
    }

    done();
};

/**
Unsubscribe to messages published to the server.

@param {String} [topic] Which messages you're no longer interested in receiving
via the `handler` function. If `topic` is `undefined` then all handlers for all
topics are unsubscribed.

@param {Function} [handler] The function you no longer want to be called with
messages published to the topic `topic`. This should be a function you've
previously passed to [`subscribe`](#mqlobberclientprototypesubscribetopic-handler-cb).
If you subscribed `handler` to a different topic then it will still be called
for messages which match that topic. If `handler` is `undefined`, all handlers
for the topic `topic` are unsubscribed.

@param {Function] [cb] Optional function to call once `handler` has been
unsubscribed from `topic` on the server. This will be passed the following
argument:

  - `{Object} err` If an error occurred then details of the error, otherwise `null`.
*/
MQlobberClient.prototype.unsubscribe = function (topic, handler, cb)
{
    if (typeof topic === 'function')
    {
        cb = topic;
        topic = undefined;
        handler = undefined;
    }

    if (this._done)
    {
        throw new Error('finished');
    }

    var ths = this, done, hdata;

    if (topic === undefined)
    {
        done = function ()
        {
            ths._subs.clear();
            ths._matcher.clear();

            if (cb)
            {
                cb();
            }
        };

        if (this._subs.size === 0)
        {
            return done();
        }
        
        hdata = new Buffer([TYPE_UNSUBSCRIBE_ALL]);
    }
    else
    {
        done = function ()
        {
            var handlers = ths._subs.get(topic);

            if (handlers)
            {
                if (handler === undefined)
                {
                    handlers.clear();
                }
                else
                {
                    handlers.delete(handler);
                }

                if (handlers.size === 0)
                {
                    ths._subs.delete(topic);
                }
            }

            ths._matcher.remove(topic, handler);

            if (cb)
            {
                cb();
            }
        };

        var handlers = this._subs.get(topic);

        if (!handlers ||
            ((handler !== undefined) &&
             ((handlers.size > 1) || !handlers.has(handler))))
        {
            return done();
        }

        hdata = Buffer.concat([new Buffer([TYPE_UNSUBSCRIBE]),
                               new Buffer(topic, 'utf8')]);
    }

    var duplex = this.mux.multiplex({ handshake_data: hdata });

    duplex.on('error', this._warning);
    duplex.on('readable', this._unexpected_data);
    duplex.end();
    
    if (cb)
    {
        return duplex.on('handshake', function (hdata)
        {
            if (hdata.length < 1)
            {
                return cb(new Error('buffer too small'));
            }

            if (hdata.readUInt8(0, true) !== 0)
            {
                return cb(new Error('server error'));
            }

            done();
        });
    }

    done();
};

/**
Publish a message to the server for interested clients to receive.

@param {String} topic Message topic. The topic should be a series of words
separated by `.` (or whatever you configured [`QlobberFSQ`](https://github.com/davedoesdev/qlobber-fsq#qlobberfsqoptions) with on the server).

@param {Object} [options] Optional settings for this publication:

  - `{Boolean} single` If `true` then the message will be given to _at most_
    one handler (across all clients connected to all servers). If you don't
    specify this then all interested handlers (across all clients) will
    receive it.

  - `{Integer} ttl` Time-to-live (in seconds) for this message. If you don't
    specify this then the default is taken from the
    [`QlobberFSQ`](https://github.com/davedoesdev/qlobber-fsq#qlobberfsqoptions) instance on the server. In any case,
    `QlobberFSQ`'s configured time-to-live is used to constrain `ttl`'s
    maximum value.
    
@param {Function] [cb] Optional function to call once the server has published
the message. This will be passed the following argument:

  - `{Object} err` If an error occurred then details of the error, otherwise `null`.

@return {Writable} Stream to which to [write](https://nodejs.org/dist/latest-v4.x/docs/api/stream.html#stream_writable_write_chunk_encoding_callback) the message's data. Make sure you [`end`](https://nodejs.org/dist/latest-v4.x/docs/api/stream.html#stream_writable_end_chunk_encoding_callback) it when you're done.
*/
MQlobberClient.prototype.publish = function (topic, options, cb)
{
    if (typeof options === 'function')
    {
        cb = options;
        options = undefined;
    }

    if (this._done)
    {
        throw new Error('finished');
    }

    options = options || {};

    var hdata = [new Buffer(2)];

    hdata[0].writeUInt8(TYPE_PUBLISH, 0);

    hdata[0].writeUInt8((options.single ? 1 : 0) |
                        ((options.ttl ? 1 : 0) << 1),
                        1);

    if (options.ttl)
    {
        hdata.push(new Buffer(4));
        hdata[1].writeUInt32BE(options.ttl, 0, true);
    }

    hdata.push(new Buffer(topic, 'utf8'));
    
    var duplex = this.mux.multiplex({ handshake_data: Buffer.concat(hdata) });

    duplex.on('error', this._warning);
    duplex.on('readable', this._unexpected_data);
    
    if (cb)
    {
        duplex.on('handshake', function (hdata)
        {
            if (hdata.length < 1)
            {
                return cb(new Error('buffer too small'));
            }

            if (hdata.readUInt8(0, true) !== 0)
            {
                return cb(new Error('server error'));
            }

            cb();
        });
    }

    return duplex;
};

exports.MQlobberClient = MQlobberClient;
