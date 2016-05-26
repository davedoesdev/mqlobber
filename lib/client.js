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
    frame = require('frame-stream'),
    util = require('util'),
    QlobberDedup = require('qlobber').QlobberDedup,
    TYPE_SUBSCRIBE = 0,
    TYPE_UNSUBSCRIBE = 1;

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

    stream.on('finish', function ()
    {
        ths._subs.clear();
        ths._done = true;
    });

    this._mux.multiplex({ handshake_data: options.handshake_data },
    function (err, duplex)
    {
        if (err)
        {
            return error.call(ths, err);
        }

        // mux emits error events on all duplexes if they have a listener
        // so listen for errors on the control duplex instead of mux now
        ths._mux.removeListener('error', error);
        duplex.on('error', error);

        ths._control = frame.encode(options);
        ths._control.on('error', error);
        ths._control.pipe(duplex);

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

            if (hdata.length === 0)
            {
                return warning.call(dplex, new Error('empty buffer'));
            }

            var info = {},
                flags = hdata.readUInt8(0, true),
                pos = 1,
                topic;

            info.single = !!(flags & 1);

            if (flags & 2)
            {
                info.expires = hdata.readUInt32BE(1, true) * Math.pow(2, 32) + 
                               hdata.readUInt32BE(5, true);
                pos += 8;
            }

            info.topic = hdata.toString('utf8', pos); 

            for (var handler of ths._matcher.match(info.topic))
            {
                handler(dplex, info);
            }
        });
    });
}

util.inherits(MQlobberClient, EventEmitter);

MQlobberClient.prototype.subscribe = function (topic, handler, cb)
{
    if (this._done)
    {
        return cb(new Error('finished'));
    }

    var handlers = this._subs.get(topic);

    if (!handlers)
    {
        handlers = new Set();
        this._control.write(Buffer.concat([new Buffer([TYPE_SUBSCRIBE]),
                                           new Buffer(topic, 'utf8')]));
        this._subs.set(topic, handlers);
    }

    handlers.add(handler);
    this._matcher.add(topic, handler);

    cb();
};

MQlobberClient.prototype.unsubscribe = function (topic, handler, cb)
{
    if (cb === undefined)
    {
        cb = handler;
        handler = undefined;
    }

    if (cb === undefined)
    {
        cb = topic;
        topic = undefined;
    }

    if (this._done)
    {
        return cb(new Error('finished'));
    }

    if (topic === undefined)
    {
        for (var t of this._subs.keys())
        {
            this._control.write(Buffer.concat([new Buffer([TYPE_UNSUBSCRIBE]),
                                               new Buffer(t, 'utf8')]));
        }

        this._subs.clear();
        this._matcher.clear();
    }
    else
    {
        var handlers = this._subs.get(topic);

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
                this._control.write(Buffer.concat([new Buffer([TYPE_UNSUBSCRIBE]),
                                                   new Buffer(topic, 'utf8')]));
                this._subs.delete(topic);
            }
        }

        this._matcher.remove(topic, handler);
    }

    cb();
};

MQlobberClient.prototype.publish = function (topic, options, cb)
{
    if (cb === undefined)
    {
        cb = options;
        options = undefined;
    }

    if (this._done)
    {
        return cb(new Error('finished'));
    }

    options = options || {};

    var hdata = [new Buffer(1)],
        warning = this._warning;

    hdata[0].writeUInt8((options.single ? 1 : 0) |
                        ((options.ttl ? 1 : 0) << 1),
                        0);

    if (options.ttl)
    {
        hdata.push(new Buffer(4));
        hdata[1].writeUInt32BE(options.ttl, 0, true);
    }

    hdata.push(new Buffer(topic, 'utf8'));
    
    this._mux.multiplex({ handshake_data: Buffer.concat(hdata) },
    function (err, duplex)
    {
        if (duplex)
        {
            duplex.on('error', warning);
        }

        cb(err, duplex);
    });
};

exports.MQlobberClient = MQlobberClient;
