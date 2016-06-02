/*
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
    this._warning = warning; // for publish

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
