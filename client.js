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
        ths.emit('error', err, this);
    }

    this._mux.on('error', error);

    function done()
    {
        ths._subs.clear();
        ths._matcher.clear();
        ths._done = true;
    }

    stream.on('end', done);
    stream.on('finish', done);

    this._mux.multiplex({ handshake_data: options.handshake_data },
    function (err, duplex)
    {
        if (err)
        {
            return error.call(ths, err);
        }

        duplex.on('error', error);

        duplex.on('handshake', function (hdata)
        {
            ths.emit('handshake', hdata);
        });

        // need to handle published messages from server (handshake)

        ths._control = frame.encode(options);
        ths._control.on('error', error);
        ths._control.pipe(duplex);
    });
}

util.inherits(MQlobberClient, EventEmitter);

MQlobberClient.prototype.subscribe = function (topic, handler)
{
    if (!this._done)
    {
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
    }
};

MQlobberClient.prototype.unsubscribe = function (topic, handler)
{
    if (topic === undefined)
    {
        for (var t of this._subs.keys())
        {
            var hdata = Buffer.concat([new Buffer([TYPE_UNSUBSCRIBE]),
                                       new Buffer(t, 'utf8')]);
            this._control.write(hdata);
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
                var hdata = Buffer.concat([new Buffer([TYPE_UNSUBSCRIBE]),
                                           new Buffer(topic, 'utf8')]);
                this._control.write(hdata);
                this._subs.delete(topic);
            }
        }

        this._matcher.remove(topic, handler);
    }
};

MQlobberClient.prototype.publish = function (topic, options, cb)
{
    if (cb === undefined)
    {
        cb = options;
        options = undefined;
    }

    options = options || {};

    var hdata = Buffer.concat([new Buffer([options.single ? 1 : 0]),
                               new Buffer(topic, 'utf8')]);

    this._mux.multiplex({ handshake_data: hdata }, cb);
};

