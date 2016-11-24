"use strict";

var EventEmitter = require('events').EventEmitter,
    BPMux = require('bpmux').BPMux,
    util = require('util'),
    TYPE_SUBSCRIBE = 0,
    TYPE_UNSUBSCRIBE = 1,
    TYPE_UNSUBSCRIBE_ALL = 2,
    TYPE_PUBLISH = 3;

/**
Create a new `MQlobberServer` object for publishing and subscribing to messages
on behalf of a client.

@constructor

@param {QlobberFSQ} fsq File system queue - an instance of
[`QlobberFSQ`](https://github.com/davedoesdev/qlobber-fsq#qlobberfsqoptions).
This does the heavy-lifting of reading and writing messages to a directory on
the file system.

@param {Duplex} stream Connection to the client. The client should use
[`MQlobberClient`](#mqlobberclientstream-options) on its side of the connection.
How the connection is made is up to the caller - it just has to supply a
[`Duplex`](https://nodejs.org/dist/latest-v4.x/docs/api/stream.html#stream_class_stream_duplex). For example, [`net.Socket`](https://nodejs.org/dist/latest-v4.x/docs/api/net.html#net_class_net_socket) or [`PrimusDuplex`](https://github.com/davedoesdev/primus-backpressure#primusduplexmsg_stream-options).

@param {Object} [options] Configuration options. This is passed down to
[`BPMux`](https://github.com/davedoesdev/bpmux#bpmuxcarrier-options)
(which multiplexes message streams over the connection to the
client). It also supports the following additional property:

  - `{Boolean} send_expires` Whether to include message expiry time in metadata
    sent to the client. Defaults to `false`.
*/
function MQlobberServer(fsq, stream, options)
{
    EventEmitter.call(this);

    options = options || {};

    this.fsq = fsq;
    this._subs = new Set();
    this._done = false;
    this.mux = new BPMux(stream, util._extend(util._extend({}, options),
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
        ths.unsubscribe();
        ths._done = true;
    });

    function relay_error(err)
    {
        /*jshint validthis: true */

        warning.call(this, err);

        switch (this._readableState.pipesCount)
        {
            case 0:
                break;

            case 1:
                this._readableState.pipes.emit('error', err);
                break;

            default:
                var dests = new Set(), dest;
                for (dest of this._readableState.pipes)
                {
                    dests.add(dest);
                }
                for (dest of dests)
                {
                    dest.emit('error', err);
                }
                break;
        }
    }

    this.handler = function (data, info, cb)
    {
        data.on('error', relay_error);

        if (!info.single)
        {
            data.on('end', cb);
        }

        var hdata = [new Buffer(1)];

        hdata[0].writeUInt8((info.single ? 1 : 0) |
                            ((options.send_expires ? 1 : 0) << 1) |
                            ((info.existing ? 1 : 0) << 2),
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
        
        function multiplex(on_error)
        {
            var duplex = ths.mux.multiplex(
            {
                handshake_data: Buffer.concat(hdata)
            });

            duplex.on('error', function ()
            {
                this.end();
            });

            duplex.on('error', on_error || cb);
            
            duplex.on('readable', function ()
            {
                while (true)
                {
                    if (this.read() === null)
                    {
                        break;
                    }

                    warning.call(this, new Error('unexpected data'));
                }
            });

            if (info.single)
            {
                var end = function()
                {
                    cb(new Error('ended before handshaken'));
                };

                duplex.on('end', end);

                duplex.on('handshake', function (hdata)
                {
                    this.removeListener('end', end);

                    if (hdata.length < 1)
                    {
                        return cb(new Error('buffer too small'));
                    }

                    if (hdata.readUInt8(0, true) !== 0)
                    {
                        return cb(new Error('client error'));
                    }

                    ths.emit('ack', info);

                    cb();
                });
            }
            
            return duplex;
        }

        if (!ths.emit('message', data, info, multiplex, cb))
        {
            try
            {
                data.pipe(multiplex());
            }
            catch (ex)
            {
                cb(ex);
            }
        }
    };

    this.handler.accept_stream = true;
    this.handler.mqlobber_server = this;

    function end()
    {
        /*jshint validthis: true */
        this.emit('error', new Error('ended before handshaken'));
    }

    this.mux.on('end', end);

    this.mux.once('handshake', function (duplex, hdata, delay)
    {
        this.removeListener('end', end);

        duplex.on('error', warning);
        duplex.end(); // currently no data on initial duplex

        this.on('handshake', function (duplex, hdata, delay)
        {
            if (!delay)
            {
                // duplex was initiated by us (outgoing message)
                return;
            }

            function dend()
            {
                /*jshint validthis: true */
                this.end();
            }

            // only read from incoming duplex but don't end until all data is
            // read in order to allow application to apply back-pressure
            duplex.on('end', dend);
            duplex.on('error', dend);
            duplex.on('error', relay_error);

            var handshake = delay();

            function done(err, data)
            {
                if (err)
                {
                    warning.call(duplex, err);
                }

                var hdata = new Buffer(1);
                hdata.writeUInt8(err ? 1 : 0);
                handshake(Buffer.isBuffer(data) ? Buffer.concat([hdata, data]) : hdata);

                duplex.on('readable', function ()
                {
                    while (true)
                    {
                        if (this.read() === null)
                        {
                            break;
                        }

                        warning.call(this, new Error('unexpected data'));
                    }
                });
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
                    if (!ths.emit('pre_subscribe_requested', topic, done) &&
                        !ths.emit('subscribe_requested', topic, done))
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
                    if (hdata.length < 2)
                    {
                        return done(new Error('buffer too small'));
                    }

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
                                hdata.readUInt32BE(2, true) * 1000);
                    }

                    topic = hdata.toString('utf8', pos);

                    if (!ths.emit('pre_publish_requested', topic, duplex, options, done) &&
                        !ths.emit('publish_requested', topic, duplex, options, done))
                    {
                        duplex.pipe(ths.fsq.publish(topic, options, done));
                    }

                    break;

                default:
                    done(new Error('unknown type: ' + type));
                    break;
            }
        });

        ths.emit('handshake', hdata, delay);
    });
}

util.inherits(MQlobberServer, EventEmitter);

/**
Subscribe the connected client to messages.

Note: If the client is already subscribed to `topic`, this function will do
nothing (other than call `cb`).

@param {String} topic Which messages the client should receive. Message topics
are split into words using `.` as the separator. You can use `*` to match
exactly one word in a topic or `#` to match zero or more words. For example,
`foo.*` would match `foo.bar` whereas `foo.#` would match `foo`, `foo.bar` and
`foo.bar.wup`. Note these are the default separator and wildcard characters.
They can be changed when [constructing the `QlobberFSQ` instance]
(https://github.com/davedoesdev/qlobber-fsq#qlobberfsqoptions) passed to
`MQlobberServer`'s [constructor](#mqlobberserverfsq-stream-options).

@param {Object} [options] Optional settings for this subscription:

  - `{Boolean} subscribe_to_existing` If `true` then the client will be sent
    any existing, unexpired messages that match `topic`, as well as new ones.
    Defaults to `false` (only new messages).

@param {Function} [cb] Optional function to call once the subscription has been
made. This will be passed the following argument:

  - `{Object} err` If an error occurred then details of the error, otherwise `null`.
*/
MQlobberServer.prototype.subscribe = function (topic, options, cb)
{
    if (typeof options === 'function')
    {
        cb = options;
        options = undefined;
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

    if (!this._done && !this._subs.has(topic))
    {
        this.fsq.subscribe(topic, this.handler, options, function (err)
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

/**
Unsubscribe the connected client from messages.

@param {String} [topic] Which messages the client should no longer receive.
If topic is `undefined` then the client will receive no more messages at all.

@param {Function} [cb] Optional function to call once the subscription has been
made. This will be passed the following argument:

  - `{Object} err` If an error occurred then details of the error, otherwise `null`'.
*/
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
            ths.fsq.unsubscribe(t, ths.handler, function (err)
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
        this.fsq.unsubscribe(topic, this.handler, function (err)
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
