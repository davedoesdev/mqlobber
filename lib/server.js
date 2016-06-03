/*
Just supply qlobber-fsq obj and stream. Multiplex (using bpmux) off that.
No user/auth stuff here.

Messages (published and received) on separate streams. Use handshake data
to describe the messages.

Subscribe, unsubscribe also on a stream each.

Also allow subscribe, unsubscribe from not in the stream.

So take:

- fsq object; up to caller to specify dedup - without, client may receive
  messages multiple times
- stream
- options for bpmux

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
    util = require('util'),
    TYPE_SUBSCRIBE = 0,
    TYPE_UNSUBSCRIBE = 1,
    TYPE_UNSUBSCRIBE_ALL = 2,
    TYPE_PUBLISH = 3;

function MQlobberServer(fsq, stream, options)
{
    EventEmitter.call(this);

    options = options || {};

    this._fsq = fsq;
    this._subs = new Set();
    this._done = false;
    this._mux = new BPMux(stream, util._extend(util._extend({}, options),
                {
                    high_channels: true
                }));

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
        ths.unsubscribe();
        ths._done = true;
    });

    this._handler = function (data, info, cb)
    {
        data.on('error', warning);

        var hdata = [new Buffer(1)];

        hdata[0].writeUInt8((info.single ? 1 : 0) |
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
        
        var duplex = ths._mux.multiplex(
        {
            handshake_data: Buffer.concat(hdata)
        });

        duplex.on('error', warning);
        data.on('end', cb);

        if (!ths.emit('message', data, duplex, info))
        {
            data.pipe(duplex);
        }
    };

    this._handler.accept_stream = true;
    this._handler.mqlobber_stream = stream;

    this._mux.once('handshake', function (duplex, hdata, delay)
    {
        duplex.on('error', warning);
        duplex.end(); // currently no data on initial duplex

        this.on('handshake', function (duplex, hdata, delay)
        {
            if (!delay)
            {
                // duplex was initiated by us (outgoing message)
                return;
            }

            duplex.on('error', warning);
            duplex.end(); // only read from incoming message duplex

            var handshake = delay();

            function done(err)
            {
                if (err)
                {
                    warning.call(duplex, err);
                }

                var hdata = new Buffer(1);
                hdata.writeUInt8(err ? 1 : 0);
                handshake(hdata);
            }

            if (hdata.length < 1)
            {
                return done(new Error('buffer too small'));
            }

            var type = hdata.readUInt8(0, true),
                topic;

            switch (type)
            {
                case TYPE_SUBSCRIBE:
                    topic = hdata.toString('utf8', 1);
                    if (!ths.emit('subscribe_requested', topic, done))
                    {
                        ths.subscribe(topic, done);
                    }
                    break;

                case TYPE_UNSUBSCRIBE:
                    topic = hdata.toString('utf8', 1);
                    if (!ths.emit('unsubscribe_requested', topic, done))
                    {
                        ths.unsubscribe(topic, done);
                    }
                    break;

                case TYPE_UNSUBSCRIBE_ALL:
                    if (!ths.emit('unsubscribe_all_requested', done))
                    {
                        ths.unsubscribe(done);
                    }
                    break;

                case TYPE_PUBLISH:
                    var options = {},
                        flags = hdata.readUInt8(1, true),
                        pos = 2;

                    options.single = !!(flags & 1);

                    if (flags & 2)
                    {
                        pos += 4;

                        if (hdata.length < pos)
                        {
                            return done(new Error('buffer too small'));
                        }

                        options.ttl = Math.min(
                                options.single ? fsq._single_ttl : fsq._multi_ttl,
                                hdata.readUInt32BE(pos, true) * 1000);
                    }

                    topic = hdata.toString('utf8', pos);

                    if (!ths.emit('publish_requested', topic, duplex, options, done))
                    {
                        duplex.pipe(ths._fsq.publish(topic, options, done));
                    }

                    break;

                default:
                    done(new Error('unknown type:' + type));
                    break;
            }
        });

        ths.emit('handshake', hdata, delay);
    });
}

util.inherits(MQlobberServer, EventEmitter);

MQlobberServer.prototype.subscribe = function (topic, cb)
{
    var ths = this;

    function cb2(err)
    {
        if (cb)
        {
            cb(err);
        }
        else if (err)
        {
            ths._warning(err);
        }
    }

    if (!this._done && !this._subs.has(topic))
    {
        this._fsq.subscribe(topic, this._handler, function (err)
        {
            if (!err)
            {
                ths._subs.add(topic);
            }
            cb2(err);
        });
    }
    else
    {
        cb2();
    }
};

MQlobberServer.prototype.unsubscribe = function (topic, cb)
{
    if (typeof topic === 'function')
    {
        cb = topic;
        topic = undefined;
    }

    var ths = this;

    function cb2(err)
    {
        if (cb)
        {
            cb(err);
        }
        else if (err)
        {
            ths._warning(err);
        }
    }

    if (topic === undefined)
    {
        var unsub = function (t)
        {
            ths._fsq.unsubscribe(t, ths._handler, function (err)
            {
                if (err)
                {
                    cb2(err);
                }
                else
                {
                    ths._subs.delete(t);
                    process.nextTick(next);
                }
            });
        },

        next = function ()
        {
            for (var t of ths._subs)
            {
                return unsub(t);
            }
            cb2();
        };

        next();
    }
    else if (this._subs.has(topic))
    {
        this._fsq.unsubscribe(topic, this._handler, function (err)
        {
            if (!err)
            {
                ths._subs.delete(topic);
            }
            cb2(err);
        });
    }
    else
    {
        cb2();
    }
};

exports.MQlobberServer = MQlobberServer;
