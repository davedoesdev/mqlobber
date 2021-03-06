"use strict";

var EventEmitter = require('events').EventEmitter,
    BPMux = require('bpmux').BPMux,
    util = require('util'),
    TYPE_SUBSCRIBE = 0,
    TYPE_UNSUBSCRIBE = 1,
    TYPE_UNSUBSCRIBE_ALL = 2,
    TYPE_PUBLISH = 3,
    thirty_two_bits = Math.pow(2, 32);

/**
Create a new `MQlobberServer` object for publishing and subscribing to messages
on behalf of a client.

@constructor

@param {QlobberFSQ|QlobberPG} fsq File system queue - an instance of [`QlobberFSQ`](https://github.com/davedoesdev/qlobber-fsq#qlobberfsqoptions).  This does the heavy-lifting of reading and writing messages to a directory on the file system. Alternatively, you can pass an instance of [`QlobberPG`](https://github.com/davedoesdev/qlobber-pg), which uses PostgreSQL to process messages.

@param {Duplex} stream Connection to the client. The client should use [`MQlobberClient`](#mqlobberclientstream-options) on its side of the connection.  How the connection is made is up to the caller - it just has to supply a [`Duplex`](https://nodejs.org/dist/latest-v4.x/docs/api/stream.html#stream_class_stream_duplex). For example, [`net.Socket`](https://nodejs.org/dist/latest-v4.x/docs/api/net.html#net_class_net_socket) or [`PrimusDuplex`](https://github.com/davedoesdev/primus-backpressure#primusduplexmsg_stream-options).

@param {Object} [options] Configuration options. This is passed down to [`BPMux`](https://github.com/davedoesdev/bpmux#bpmuxcarrier-options) (which multiplexes message streams over the connection to the client). It also supports the following additional properties:
- `{Boolean} send_expires` Whether to include message expiry time in metadata sent to the client. Defaults to `false`.

- `{Boolean} send_size` Whether to include message size in metadata sent to then client. Defaults to `false`.

- `{Boolean} defer_to_final_handler` If `true` then a message stream is only considered finished when all `MQlobberServer` objects finish processing it. Defaults to `false`.
*/
function MQlobberServer(fsq, stream, options)
{
    EventEmitter.call(this);

    options = options || {};

    this.fsq = fsq;
    this.subs = new Set();
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
        if (err && !ths.emit('warning', err, this))
        {
            console.error(err);
        }
    }
    this._warning = warning;

    this.mux.on('error', error);

    function handshake_sent(duplex, complete)
    {
        if (!complete)
        {
            ths.emit('backoff');
        }
    }

    this.mux.on('handshake_sent', handshake_sent);
    this.mux.on('pre_handshake_sent', handshake_sent);

    this.mux.on('drain', function ()
    {
        ths.emit('drain');
    });

    this.mux.on('full', function ()
    {
        ths.emit('full');
    });

    this.mux.on('removed', function (duplex)
    {
        ths.emit('removed', duplex);
    });

    this.mux.on('finish', function ()
    {
        ths._done = true;

        if (!ths.emit('unsubscribe_all_requested', warning))
        {
            ths.default_unsubscribe_all_requested_handler();
        }
    });

    this.relay_error = function (err)
    {
        /*jshint validthis: true */
        /* istanbul ignore else */
        if (this._readableState.pipes)
        {
            // In Node 12, a single pipe isn't in an array
            [].concat(this._readableState.pipes).forEach(function (dest)
            {
                dest.emit('error', err);
            });
        }
    };

    this.handler = function (data, info, cb3)
    {
        var called = false;

        function cb(err, cb2)
        {
            warning(err);

            if (!called)
            {
                cb3.num_handlers -= 1;
                called = true;
            }

            if (options.defer_to_final_handler && (cb3.num_handlers > 0))
            {
                if (cb2)
                {
                    return cb2(err);
                }
                return;
            }

            cb3(err, cb2);
        }

        data.on('error', warning);
        data.on('error', ths.relay_error);

        if (!info.single)
        {
            data.on('end', cb);
        }

        var hdata = [Buffer.alloc(1)], buf;

        hdata[0].writeUInt8((info.single ? 1 : 0) |
                            ((options.send_expires ? 1 : 0) << 1) |
                            ((info.existing ? 1 : 0) << 2) |
                            ((options.send_size ? 1 : 0) << 3),
                            0);

        if (options.send_expires)
        {
            buf = Buffer.alloc(8);
            var expires = Math.floor(info.expires / 1000);
            // 32 bits are only enough for dates up to 2106
            // can't use >> and & because JS converts to 32 bits for bitwise ops
            buf.writeUInt32BE(Math.floor(expires / thirty_two_bits), 0);
            buf.writeUInt32BE(expires % thirty_two_bits, 4);
            hdata.push(buf);
        }

        if (options.send_size)
        {
            buf = Buffer.alloc(8);
            buf.writeUInt32BE(Math.floor(info.size / thirty_two_bits), 0);
            buf.writeUInt32BE(info.size % thirty_two_bits, 4);
            hdata.push(buf);
        }
        
        hdata.push(Buffer.from(info.topic, 'utf8'));
        
        function multiplex(on_error)
        {
            var duplex = ths.mux.multiplex(Object.assign({}, options,
            {
                handshake_data: Buffer.concat(hdata)
            }));

            duplex.on('error', function ()
            {
                this.peer_error_then_end();
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

                    duplex.emit('ack', info);
                    ths.emit('ack', info);

                    cb();
                });
            }
            
            return duplex;
        }

        multiplex.num_handlers = cb3.num_handlers;

        if (!ths.emit('message', data, info, multiplex, cb))
        {
            ths.default_message_handler(data, info, multiplex, cb);
        }
    };

    this.handler.accept_stream = true;
    this.handler.mqlobber_server = this;

    function end()
    {
        /*jshint validthis: true */
        error.call(this, new Error('ended before handshaken'));
    }

    this.mux.on('end', end);

    this.mux.on('peer_multiplex', function (duplex)
    {
        duplex.on('error', warning);
    });

    this.mux.once('handshake', function (duplex, hdata, delay)
    {
        this.removeListener('end', end);

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
            duplex.on('error', ths.relay_error);

            var handshake = delay();

            function done(err)
            {
                warning.call(duplex, err);

                var hdata = Buffer.alloc(1);
                hdata.writeUInt8(err ? 1 : 0);

                if (arguments.length > 1)
                {
                    var data = arguments[arguments.length - 1];
                    if (Buffer.isBuffer(data))
                    {
                        hdata = Buffer.concat([hdata, data]);
                    }
                }

                handshake(hdata);
                
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
                        ths.default_subscribe_requested_handler(topic, done);
                    }
                    break;

                case TYPE_UNSUBSCRIBE:
                    topic = hdata.toString('utf8', 1);
                    if (!ths.emit('pre_unsubscribe_requested', topic, done) &&
                        !ths.emit('unsubscribe_requested', topic, done))
                    {
                        ths.default_unsubscribe_requested_handler(topic, done);
                    }
                    break;

                case TYPE_UNSUBSCRIBE_ALL:
                    if (!ths.emit('unsubscribe_all_requested', done))
                    {
                        ths.default_unsubscribe_all_requested_handler(done);
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
                        ths.default_publish_requested_handler(topic, duplex, options, done);
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

@param {String} topic Which messages the client should receive. Message topics are split into words using `.` as the separator. You can use `*` to match exactly one word in a topic or `#` to match zero or more words. For example, `foo.*` would match `foo.bar` whereas `foo.#` would match `foo`, `foo.bar` and `foo.bar.wup`. Note these are the default separator and wildcard characters.  They can be changed when [constructing the `QlobberFSQ` instance] (https://github.com/davedoesdev/qlobber-fsq#qlobberfsqoptions) passed to `MQlobberServer`'s [constructor](#mqlobberserverfsq-stream-options).

@param {Object} [options] Optional settings for this subscription:
- `{Boolean} subscribe_to_existing` If `true` then the client will be sent any existing, unexpired messages that match `topic`, as well as new ones.  Defaults to `false` (only new messages).

@param {Function} [cb] Optional function to call once the subscription has been made. This will be passed the following arguments:
- `{Object} err` If an error occurred then details of the error, otherwise `null`.

- `{Integer} n` The number of subscriptions made (0 if `topic` was already subscribed to, 1 if not).
*/
MQlobberServer.prototype.subscribe = function (topic, options, cb)
{
    if (typeof options === 'function')
    {
        cb = options;
        options = undefined;
    }

    var ths = this;

    function cb2(err, n)
    {
        if (cb)
        {
            cb(err, n);
        }
        else
        {
            ths._warning(err);
        }
    }

    if (!this._done && !this.subs.has(topic))
    {
        // assumes fsq.subscribe calls back synchronously
        this.fsq.subscribe(topic, this.handler, options, function (err)
        {
            if (err)
            {
                return cb2(err, 0);
            }

            ths.subs.add(topic);
            cb2(null, 1);
        });
    }
    else
    {
        cb2(null, 0);
    }
};

/**
Unsubscribe the connected client from messages.

@param {String} [topic] Which messages the client should no longer receive.  If topic is `undefined` then the client will receive no more messages at all.

@param {Function} [cb] Optional function to call once the subscription has been removed. This will be passed the following arguments:
- `{Object} err` If an error occurred then details of the error, otherwise `null`'.

- `{Integer} n` The number of subscriptions removed.
*/
MQlobberServer.prototype.unsubscribe = function (topic, cb)
{
    if (typeof topic === 'function')
    {
        cb = topic;
        topic = undefined;
    }

    var ths = this;

    function cb2(err, n)
    {
        if (cb)
        {
            cb(err, n);
        }
        else
        {
            ths._warning(err);
        }
    }

    if (topic === undefined)
    {
        var count = 0,
        
        unsub = function (t)
        {
            // assumes fsq.unsubscribe calls back synchronously
            ths.fsq.unsubscribe(t, ths.handler, function (err)
            {
                if (err)
                {
                    return cb2(err, count);
                }

                ths.subs.delete(t);
                count += 1;

                process.nextTick(next);
            });
        },

        next = function ()
        {
            for (var t of ths.subs)
            {
                return unsub(t);
            }
            cb2(null, count);
        };

        next();
    }
    else if (this.subs.has(topic))
    {
        // assumes fsq.unsubscribe calls back synchronously
        this.fsq.unsubscribe(topic, this.handler, function (err)
        {
            if (err)
            {
                return cb2(err, 0);
            }

            ths.subs.delete(topic);
            cb2(null, 1);
        });
    }
    else
    {
        cb2(null, 0);
    }
};

MQlobberServer.prototype.default_message_handler = function (data, info, multiplex, cb)
{
    try
    {
        data.pipe(multiplex());
    }
    catch (ex)
    {
        cb(ex);
    }
};

MQlobberServer.prototype.default_subscribe_requested_handler = function (topic, done)
{
    this.subscribe(topic, done);
};

MQlobberServer.prototype.default_unsubscribe_requested_handler = function (topic, done)
{
    this.unsubscribe(topic, done);
};

MQlobberServer.prototype.default_unsubscribe_all_requested_handler = function (done)
{
    this.unsubscribe(done);
};

MQlobberServer.prototype.default_publish_requested_handler = function (topic, duplex, options, done)
{
    duplex.pipe(this.fsq.publish(topic, options, done));
};

exports.MQlobberServer = MQlobberServer;
