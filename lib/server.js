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
- options for bpmux, frame-stream

Exposes:

- subscribe (doesn't take handler)
- unsubscribe (doesn't take handler) - no topic means unsubscribe all
- publish
- events:
  - subscribe_requested (if no handler then calls subscribe)
  - unsubscribe_requested (if no handler then calls unsubscribe)
  - publish_requested (if no handler then calls publish)
  - handshake (on control channel only)
  - message (with message stream, dest stream and message info)
      - default behaviour is to pipe message stream to dest stream
      - caller can use fastest-writable or whatever they want
        (e.g. throttling, timeout, minimum data rate)

Lifecycle:

- Don't replicate lifecycle events of fsq obj or stream
- Only care about cleaning up our own state. 
  - On stream end or finish, unsubscribe all

Misc:

- Need to remember all active subscriptions so can remove when unsubscribe all
  Might as well use this to stop multiple subscriptions for same subject
  (although fsq dedup also takes care of that)
*/

"use strict";

var EventEmitter = require('events').EventEmitter,
    BPMux = require('bpmux').BPMux,
    frame = require('frame-stream'),
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
        mux = new BPMux(stream, util._extend(util._extend({}, options),
        {
            high_channels: true
        }));
        
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

        var hdata = [new Buffer(1)];

        hdata[0].writeUInt8((options.single ? 1 : 0) |
                            ((options.send_expires ? 1 : 0) << 1),
                            0);

        if (options.send_expires)
        {
            var expires = Math.floor(info.expires / 1000);
            hdata.push(new Buffer(8));
            // 32 bits are only enough for dates up to 2106
            // can't use >> and & because JS converts to 32 bits for bitwise ops
            hdata[1].writeUInt32BE(Math.floor(expires / Math.pow(2, 32)), 0);
            hdata[1].writeUInt32BE(expires % Math.pow(2, 32), 4);
        }
        
        hdata.push(new Buffer(info.topic, 'utf8'));
        
        mux.multiplex({ handshake_data: Buffer.concat(hdata) },
        function (err, duplex)
        {
            if (err)
            {
                return error.call(ths, err);
            }

            duplex.on('error', warning);
            data.on('end', cb);

            if (!ths.emit('message', data, duplex, info))
            {
                data.pipe(duplex);
            }
        });
    };

    this._handler.accept_stream = true;
    this._handler.mqlobber_stream = stream;

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
            if (data === null) { return; }

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

            var options = {},
                flags = hdata.readUInt8(0, true),
                pos = 1,
                topic;

            options.single = !!(flags & 1);

            if (flags & 2)
            {
                options.ttl = Math.min(
                        options.single ? fsq._single_ttl : fsq._multi_ttl,
                        hdata.readUInt32BE(1, true) * 1000);
                pos += 4;
            }

            topic = hdata.toString('utf8', pos);

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

exports.MQlobberServer = MQlobberServer;
