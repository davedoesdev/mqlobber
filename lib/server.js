/*
Just supply qlobber-fsq obj and stream. Multiplex (using bpmux) off that.
No user/auth stuff here.

Messages (published and received) on separate streams. Use handshake data
to describe the messages.

Subscribe, unsubscribe on a control channel. Use frame-stream

Also allow subscribe, unsubscribe from not in the stream.

So take:

- fsq object; up to caller to specify dedup - without, client may receive
  messages multiple times
- stream
- options:
    - whether to use fastest-writable
    - options for bpmux, frame-stream, fastest-writable

Exposes:

- subscribe (doesn't take handler)
- unsubscribe (doesn't take handler) - no topic means unsubscribe all
- publish
- events:
  - subscribe_requested (if no handler then calls subscribe)
  - unsubscribe_requested (if no handler then calls unsubscribe)
  - publish_requested (if no handler then calls publish)
  - handshake (on control channel only)

Lifecycle:

- Don't replicate lifecycle events of fsq obj or stream
- Only care about cleaning up our own state. 
  - On stream end or finish, unsubscribe all

Misc:

- Need to remember all active subscriptions so can remove when unsubscribe all
  Might as well use this to stop multiple subscriptions for same subject
  (although fsq dedup also takes care of that)

- How use fastest-writable? We'll have multiple mqlobber objects each with
  their own handler, so the data will be written at the speed of the slowest.
  We should put a fastest-writable onto the stream as a property, and pipe the
  stream onto it (first time we create the fw). Add our stream as peer on fw.
  (Alternative is to use info to store fw).
    - Make this optional

- We want to use the filter handler function to prevent the message being
  delivered if all streams are full.
    - How detect stream is full? write empty data
    - But the filter handler is passed when the fsq is constructed.
      This is optional behaviour, expose a filter function which caller
      can use when constructing fsq
*/

"use strict";

var EventEmitter = require('events').EventEmitter,
    BPMux = require('bpmux').BPMux,
    frame = require('frame-stream'),
    FastestWritable = require('fastest-writable').FastestWritable,
    util = require('util'),
    TYPE_SUBSCRIBE = 0,
    TYPE_UNSUBSCRIBE = 1;

function MQlobberServer(fsq, stream, options)
{
    EventEmitter.call(this);

    options = options || {};

    this._fsq = fsq;
    this._subs = new Set();
    this._done = false;

    var ths = this,
        mux = new BPMux(stream, options);

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

    mux.on('error', error);

    stream.on('finish', function ()
    {
        ths.unsubscribe();
        ths._done = true;
    });

    this._handler = function (data, info, cb)
    {
        data.on('error', warning);

        var hdata = Buffer.concat([new Buffer([info.single ? 1 : 0]),
                                   new Buffer(info.topic, 'utf8')]);

        mux.multiplex({ handshake_data: hdata }, function (err, duplex)
        {
            if (err)
            {
                return error.call(ths, err);
            }

            duplex.on('error', warning);
            data.on('end', cb);

            if (options.fastest_writable)
            {
                if (!data._mqlobber_fastest_writable)
                {
                    data._mqlobber_fastest_writable = new FastestWritable();
                    data._mqlobber_fastest_writable.on('error', warning);
                    data.pipe(data._mqlobber_fastest_writable);
                    data.emit('');
                }

                data._mqlobber_fastest_writable.add_peer(duplex);
            }
            else
            {
                data.pipe(duplex);
                data.emit('');
            }

            // what
        });
    };

    this._handler.accept_stream = true;
    this._handler._mqlobber_stream = stream;

    mux.once('handshake', function (duplex, hdata, delay)
    {
        // mux emits error events on all duplexes if they have a listener
        // so listen for errors on the control duplex instead of mux now
        mux.removeListener('error', error);
        duplex.on('error', error);

        var control = frame.decode(options);
        control.on('error', error);

        duplex.pipe(control);
        duplex.end(); // only read from control duplex

        control.on('readable', function ()
        {
            var data = this.read();

            if (data.length === 0)
            {
                return error.call(control, new Error('empty buffer'));
            }

            var type = data.readUInt8(0, true), topic;

            switch (type)
            {
                case TYPE_SUBSCRIBE:
                    topic = data.toString('utf8', 1);
                    if (!ths.emit('subscribe_requested', topic))
                    {
                        ths.subscribe(topic);
                    }
                    break;

                case TYPE_UNSUBSCRIBE:
                    topic = data.toString('utf8', 1);
                    if (!ths.emit('unsubscribe_requested', topic))
                    {
                        ths.unsubscribe(topic);
                    }
                    break;

                default:
                    error.call(control, new Error('unknown type:' + type));
                    break;
            }
        });

        this.on('handshake', function (duplex, hdata, delay)
        {
            if (!delay)
            {
                // duplex was initiated by us (outgoing message)
                return;
            }

            duplex.on('error', warning);
            duplex.end(); // only read from incoming message duplex

            if (hdata.length === 0)
            {
                return warning.call(duplex, new Error('empty buffer'));
            }

            var options = { single: !!hdata.readUInt8(0, true) },
                topic = hdata.toString('utf8', 1);

            if (!ths.emit('publish_requested', topic, duplex, options))
            {
                duplex.pipe(ths._fsq.publish(topic, options));
            }
        });

        ths.emit('handshake', hdata, delay);
    });
}

util.inherits(MQlobberServer, EventEmitter);

MQlobberServer.prototype.subscribe = function (topic)
{
    if (!this._done && !this._subs.has(topic))
    {
        this._fsq.subscribe(topic, this._handler);
        this._subs.add(topic);
    }
};

MQlobberServer.prototype.unsubscribe = function (topic)
{
    if (topic === undefined)
    {
        for (var t of this._subs)
        {
            this._fsq.unsubscribe(t, this._handler);
        }
        this._subs.clear();
    }
    else if (this._subs.has(topic))
    {
        this._fsq.unsubscribe(topic, this._handler);
        this._subs.delete(topic);
    }
};

MQlobberServer.filter_all_drained = function (info, handlers, cb)
{
    for (var h of handlers)
    {
        if (h._mqlobber_stream && !h._mqlobber_stream.write(''))
        {
            return cb(null, false);
        }
    }

    cb(null, true, handlers);
};

exports.MQlobberServer = MQlobberServer;
