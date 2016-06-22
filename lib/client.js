/*
# mqlobber&nbsp;&nbsp;&nbsp;[![Build Status](https://travis-ci.org/davedoesdev/mqlobber.png)](https://travis-ci.org/davedoesdev/mqlobber) [![Coverage Status](https://coveralls.io/repos/davedoesdev/mqlobber/badge.png?branch=master&service=github)](https://coveralls.io/r/davedoesdev/mqlobber?branch=master) [![NPM version](https://badge.fury.io/js/mqlobber.png)](http://badge.fury.io/js/mqlobber)

Streaming message queue with pub-sub, work queues, wildcards and back-pressure.
Just Node and a filesystem required.

`mqlobber` basically remotes 
[`qlobber-fsq`](https://github.com/davedoesdev/qlobber-fsq) over one or more
connections.

<p align="center"><img src="diagrams/overview.svg" width="80%"/></p>

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

<p align="center"><img src="diagrams/multi_instance.svg" width="80%"/></p>

No other backend services are required - just Node and a filesystem.

The API is described [here](#tableofcontents).

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







Take:

- stream
- optional handshake data for the control channel

Exposes:

- subscribe (topic, handler)
- unsubscribe (topic, handler)
- publish (topic, payload, options)
- events:
    - handshake

Need to remember all active subscriptions so:

- don't send subscribe message if already subscribed
- can remove when unsubscribe all

We're going to need our own QlobberDedup to put our handlers on:

- We'll just get a topic back from the server
- Need to get list of handlers to call for the topic

This needs to be usable in browser.

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

function MQlobberClient(stream, options)
{
    EventEmitter.call(this);

    options = options || {};

    this._subs = new Map();
    this._matcher = new QlobberDedup(options);
    this._mux = new BPMux(stream, options);
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

    this._mux.on('error', error);

    this._mux.on('handshake_sent', function (duplex, complete)
    {
        if (!complete)
        {
            ths.emit('full');
        }
    });

    stream.on('finish', function ()
    {
        ths._subs.clear();
        ths._done = true;
    });

    var duplex = this._mux.multiplex({ handshake_data: options.handshake_data });
    duplex.on('error', warning);
    duplex.end(); // currently no data on initial duplex

    ths._mux.on('handshake', function (dplex, hdata, delay)
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

    var duplex = this._mux.multiplex(
    {
        handshake_data: Buffer.concat([new Buffer([TYPE_SUBSCRIBE]),
                                       new Buffer(topic, 'utf8')])
    });

    duplex.on('error', this._warning);
    duplex.on('readable', this._unexpected_data);

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

    var duplex = this._mux.multiplex({ handshake_data: hdata });

    duplex.on('error', this._warning);
    duplex.on('readable', this._unexpected_data);
    
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
    
    var duplex = this._mux.multiplex({ handshake_data: Buffer.concat(hdata) });

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
