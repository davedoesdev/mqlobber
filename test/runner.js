/*jshint mocha: true */
"use strict";

var stream = require('stream'),
    util = require('util'),
    crypto = require('crypto'),
    path = require('path'),
    async = require('async'),
    rimraf = require('rimraf'),
    mqlobber = require('..'),
    MQlobberClient = mqlobber.MQlobberClient,
    MQlobberServer = mqlobber.MQlobberServer,
    QlobberFSQ = require('qlobber-fsq').QlobberFSQ,
    chai = require('chai'),
    expect = chai.expect,
    sinon = require('sinon'),
    FastestWritable = require('fastest-writable').FastestWritable,
    rabbitmq_bindings = require('./rabbitmq_bindings');

function read_all(s, cb)
{
    var bufs = [];

    s.on('end', function ()
    {
        if (cb)
        {
            cb(Buffer.concat(bufs));
        }
    });

    s.on('readable', function ()
    {
        while (true)
        {
            var data = this.read();
            if (data === null) { break; }
            bufs.push(data);
        }
    });
}

function NullStream()
{
    stream.Writable.call(this);
}

util.inherits(NullStream, stream.Writable);

NullStream.prototype._write = function ()
{
};

function topic_sort(a, b)
{
    return parseInt(a.substr(1), 10) - parseInt(b.substr(1), 10);
}

var timeout = 5 * 60;

module.exports = function (type, connect_and_accept)
{
describe(type, function ()
{
    function with_mqs(n, description, f, mqit, options)
    {
        describe('mqs=' + n, function ()
        {
            this.timeout(timeout * 1000);

            var fsq, mqs, ended = false;

            before(function (cb)
            {
                var fsq_dir = path.join(path.dirname(require.resolve('qlobber-fsq')), 'fsq');
                rimraf(fsq_dir, cb);
            });

            before(function (cb)
            {
                fsq = new QlobberFSQ(util._extend(
                {
                    multi_ttl: timeout * 1000,
                    single_ttl: timeout * 2 * 1000
                }, options));

                fsq.on('start', function ()
                {
                    async.timesSeries(n, function (i, cb)
                    {
                        connect_and_accept(function (cs, ss)
                        {
                            var cmq = new MQlobberClient(cs),
                                smq = new MQlobberServer(fsq, ss,
                                      options === null ? options :
                                      util._extend(
                                      {
                                          send_expires: true
                                      }, options)),
                                info = {
                                    client: cmq,
                                    server: smq,
                                    client_stream: cs,
                                    server_stream: ss
                                };

                            if (options && options.onmade)
                            {
                                options.onmade(info);
                            }

                            if (options && options.skip_client_handshake)
                            {
                                return cb(null, info);
                            }

                            cmq.on('handshake', function ()
                            {
                                cb(null, info);
                            });
                        });
                    }, function (err, v)
                    {
                        mqs = v;
                        cb(err);
                    });
                });
            });

            function end(cb)
            {
                if (ended)
                {
                    return cb();
                }
                ended = true;

                async.each(mqs, function (mq, cb)
                {
                    if (mq.client.mux.carrier._readableState.ended ||
                        mq.client.mux.carrier.destroyed)
                    {
                        return cb();
                    }
                    mq.client_stream.on('end', cb);
                    mq.server_stream.on('end', function ()
                    {
                        this.end();
                    });
                    mq.client_stream.end();
                }, function (err)
                {
                    fsq.stop_watching(function ()
                    {
                        cb(err);
                    });
                });
            }

            after(end);

            if (options && options.sinon)
            {
                beforeEach(function ()
                {
                    this.sinon = sinon.sandbox.create();
                });

                afterEach(function ()
                {
                    this.sinon.restore();
                });
            }

            (mqit || it)(description, function (cb)
            {
                f.call(this, mqs, cb, end);
            });
        });
    }

    with_mqs(1, 'should publish and receive a message on single stream',
    function (mqs, cb)
    {
        mqs[0].client.subscribe('foo', function (s, info)
        {
            expect(info.single).to.equal(false);
            expect(info.existing).to.equal(false);
            expect(info.topic).to.equal('foo');

            expect(this).to.equal(mqs[0].client);

            var now = Date.now(), expires = info.expires * 1000;

            expect(expires).to.be.above(now);
            expect(expires).to.be.at.most(now + timeout * 1000);

            read_all(s, function (v)
            {
                expect(v.toString()).to.equal('bar');
                cb();
            });
        }, function (err, data)
        {
            if (err) { return cb(err); }
            expect(data).to.equal(undefined);
            mqs[0].client.publish('foo', function (err, data)
            {
                if (err) { return cb(err); }
                expect(data).to.equal(undefined);
            }).end('bar');
        });
    });

    with_mqs(2, 'should publish on one stream and receive on another',
    function (mqs, cb)
    {
        mqs[0].client.subscribe('foo', function (s, info)
        {
            expect(info.single).to.equal(false);
            expect(info.topic).to.equal('foo');
            read_all(s, function (v)
            {
                expect(v.toString()).to.equal('bar');
                cb();
            });
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[1].client.publish('foo', function (err, s)
            {
                if (err) { return cb(err); }
            }).end('bar');
        });
    });

    function n_streams_m_messages(n, m)
    {
        with_mqs(n, 'should publish ' + m + ' messages on each stream', function (mqs, cb)
        {
            var count_in = 0, receiveds = [];

            function check()
            {
                count_in += 1;
                //console.log('in', count_in);
                if (count_in === n * n * m)
                {
                    expect(receiveds.length).to.equal(n);
                    //console.log(receiveds);
                    for (var received of receiveds)
                    {
                        expect(received.size === n * m);
                        for (var x = 0; x < n; x += 1)
                        {
                            for (var y = 0; y < m; y += 1)
                            {
                                expect(received.get('foo.' + x + '.' + y)).to.equal('bar.' + x + '.' + y);
                            }
                        }
                    }
                    // allow time for unexpected messages
                    cb();
                }
                else if (count_in > n * n * m)
                {
                    cb(new Error('too many messages'));
                }
            }

            async.times(n, function (i, cb2)
            {
                receiveds[i] = new Map();
                mqs[i].client.subscribe('foo.#', function (s, info)
                {
                    expect(info.single).to.equal(false);
                    read_all(s, function (v)
                    {
                        receiveds[i].set(info.topic, v.toString());
                        check();
                    });
                }, cb2);
            }, function (err)
            {
                var count_out = 0;

                if (err) { return cb(err); }
                async.timesLimit(n, 10, function (i, cb3)
                {
                    async.timesLimit(m, 10, function (j, cb4)
                    {
                        mqs[i].client.publish('foo.' + i + '.' + j,
                        {
                            ttl: timeout,
                        }, function (err)
                        {
                            if (err) { return cb(err); }
                            count_out += 1;
                            //console.log('out', count_out, arguments);
                            cb4();
                        }).end('bar.' + i + '.' + j);
                    }, cb3);
                }, function (err)
                {
                    if (err) { return cb(err); }
                });
            });
        });
    }

    for (var x of [2, 5, 10, 50])
    {
        for (var y of [1, 2, 5, 10, 50])
        {
            n_streams_m_messages(x, y);
        }
    }

    with_mqs(20, 'should support multiple (but de-duplicated) subscribers',
    function (mqs, cb)
    {
        var count = 0;

        function check_err(err)
        {
            if (err) { return cb(err); }
        }

        function make_check()
        {
            return function (s)
            {
                var d = new stream.PassThrough();
                s.pipe(d);

                read_all(d, function (data)
                {
                    expect(data.toString()).to.equal('bar');
                    count += 1;
                    if (count === mqs.length * 4)
                    {
                        cb();
                    }
                    else if (count > mqs.length * 4)
                    {
                        cb(new Error('called too many times'));
                    }
                });
            };
        }

        for (var mq of mqs)
        {
            for (var topic of ['#', 'foo'])
            {
                for (var i=0; i < 2; i += 1)
                {
                    var h = make_check();
                    mq.client.subscribe(topic, h, check_err);
                    mq.client.subscribe(topic, h, check_err);
                }
            }
        }

        mqs[1].client.publish('foo', check_err).end('bar');
    });

    with_mqs(1, 'should unsubscribe handler', function (mqs, cb)
    {
        function handler1(s, info)
        {
            expect(info.single).to.equal(false);
            expect(info.topic).to.equal('foo');
            read_all(s, function (v)
            {
                expect(v.toString()).to.equal('bar');
                setTimeout(cb, 2000);
            });
        }
        function handler2()
        {
            cb(new Error('should not be called'));
        }
        mqs[0].client.subscribe('foo', handler1, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.subscribe('foo', handler2, function (err)
            {
                if (err) { return cb(err); }
                mqs[0].client.unsubscribe('foo', handler2, function (err, data)
                {
                    if (err) { return cb(err); }
                    expect(data).to.equal(undefined);
                    mqs[0].client.publish('foo', function (err)
                    {
                        if (err) { return cb(err); }
                    }).end('bar');
                });
            });
        });
    });

    with_mqs(1, 'should unsubscribe topic', function (mqs, cb)
    {
        function handler1(s, info)
        {
            expect(info.single).to.equal(false);
            expect(info.topic).to.equal('foo');
            read_all(s, function (v)
            {
                expect(v.toString()).to.equal('bar');
                setTimeout(cb, 2000);
            });
        }
        function handler2()
        {
            cb(new Error('should not be called'));
        }
        function handler3()
        {
            cb(new Error('should not be called'));
        }
        mqs[0].client.subscribe('#', handler1, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.subscribe('foo', handler2, function (err)
            {
                if (err) { return cb(err); }
                mqs[0].client.subscribe('foo', handler3, function (err)
                {
                    if (err) { return cb(err); }
                    mqs[0].client.unsubscribe('foo', undefined, function (err)
                    {
                        if (err) { return cb(err); }
                        mqs[0].client.publish('foo', function (err)
                        {
                            if (err) { return cb(err); }
                        }).end('bar');
                    });
                });
            });
        });
    });

    with_mqs(1, 'should unsubscribe all topics', function (mqs, cb)
    {
        function handler1()
        {
            cb(new Error('should not be called'));
        }
        function handler2()
        {
            cb(new Error('should not be called'));
        }
        function handler3()
        {
            cb(new Error('should not be called'));
        }
        mqs[0].client.subscribe('#', handler1, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.subscribe('foo', handler2, function (err)
            {
                if (err) { return cb(err); }
                mqs[0].client.subscribe('foo', handler3, function (err)
                {
                    if (err) { return cb(err); }
                    mqs[0].client.unsubscribe(function (err)
                    {
                        if (err) { return cb(err); }
                        mqs[0].client.publish('foo', function (err)
                        {
                            if (err) { return cb(err); }
                            setTimeout(cb, 2000);
                        }).end('bar');
                    });
                });
            });
        });
    });

    with_mqs(1, 'client should warn about empty handshake data', function (mqs, cb)
    {
        mqs[0].client.on('warning', function (err, duplex)
        {
            expect(err.message).to.equal('buffer too small');
            expect(duplex).to.be.an.instanceof(stream.Duplex);
            cb();
        });

        mqs[0].server.mux.multiplex();
    });

    with_mqs(1, 'client should warn about short handshake data', function (mqs, cb)
    {
        mqs[0].client.on('warning', function (err, duplex)
        {
            expect(err.message).to.equal('buffer too small');
            expect(duplex).to.be.an.instanceof(stream.Duplex);
            cb();
        });

        mqs[0].server.mux.multiplex(
        {
            handshake_data: new Buffer([2])
        });
    });

    with_mqs(1, 'should send server errors to client when subscribing',
    function (mqs, cb)
    {
        var got_warning = false;

        mqs[0].server.on('subscribe_requested', function (topic, done)
        {
            done(new Error('test error'));
        });

        mqs[0].server.on('warning', function (err, duplex)
        {
            expect(err.message).to.equal('test error');
            expect(duplex).to.be.an.instanceof(stream.Duplex);
            got_warning = true;
        });

        mqs[0].client.subscribe('foo', function ()
        {
            cb(new Error('should not be called'));
        }, function (err)
        {
            expect(err.message).to.equal('server error');
            expect(got_warning).to.equal(true);
            cb();
        });
    });

    with_mqs(1, 'should warn about short return handshakes from server when subscribing',
    function (mqs, cb)
    {
        mqs[0].server.on('subscribe_requested', function (topic, done)
        {
            // stop server handshake handler replying
        });

        mqs[0].server.mux.on('handshake', function (duplex, hdata, delay)
        {
            delay()(new Buffer(0));
        });

        mqs[0].client.subscribe('foo', function ()
        {
            cb(new Error('should not be called'));
        }, function (err)
        {
            expect(err.message).to.equal('buffer too small');
            cb();
        });
    });

    with_mqs(1, 'should send server errors to client when unsubscribing',
    function (mqs, cb)
    {
        var got_warning = false;

        mqs[0].server.on('unsubscribe_requested', function (topic, done)
        {
            done(new Error('test error'));
        });

        mqs[0].server.on('warning', function (err, duplex)
        {
            expect(err.message).to.equal('test error');
            expect(duplex).to.be.an.instanceof(stream.Duplex);
            got_warning = true;
        });

        mqs[0].client.subscribe('foo', function ()
        {
            cb(new Error('should not be called'));
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.unsubscribe('foo', undefined, function (err)
            {
                expect(err.message).to.equal('server error');
                expect(got_warning).to.equal(true);
                cb();
            });
        });
    });

    with_mqs(1, 'should warn about short return handshakes from server when unsubscribing',
    function (mqs, cb)
    {
        mqs[0].server.on('unsubscribe_requested', function (topic, done)
        {
            // stop server handshake handler replying
        });

        mqs[0].server.mux.on('handshake', function (duplex, hdata, delay)
        {
            delay()(new Buffer(0));
        });

        mqs[0].client.subscribe('foo', function ()
        {
            cb(new Error('should not be called'));
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.unsubscribe('foo', undefined, function (err)
            {
                expect(err.message).to.equal('buffer too small');
                cb();
            });
        });
    });

    with_mqs(1, 'should send server errors to client when publishing',
    function (mqs, cb)
    {
        var got_warning = false, got_warning2 = false, got_error = false;

        mqs[0].server.on('publish_requested', function (topic, duplex, options, done)
        {
            done(new Error('test error'));
        });

        mqs[0].server.on('warning', function (err, duplex)
        {
            expect(duplex).to.be.an.instanceof(stream.Duplex);
            expect(err.message).to.equal('test error');
            got_warning = true;

            mqs[0].server.removeAllListeners('warning');
            mqs[0].server.on('warning', function (err, duplex)
            {
                expect(duplex).to.be.an.instanceof(stream.Duplex);
                expect(err.message).to.equal('unexpected data');
                got_warning2 = true;
                if (got_error)
                {
                    cb();
                }
            });
        });

        mqs[0].client.publish('foo', function (err)
        {
            expect(err.message).to.equal('server error');
            expect(got_warning).to.equal(true);
            got_error = true;
            if (got_warning2)
            {
                cb();
            }
        }).end('bar');
    });

    with_mqs(1, 'should warn about short return handshakes from server when publishing',
    function (mqs, cb)
    {
        mqs[0].server.on('publish_requested', function (topic, done)
        {
            // stop server handshake handler replying
        });

        mqs[0].server.mux.on('handshake', function (duplex, hdata, delay)
        {
            delay()(new Buffer(0));
        });

        mqs[0].client.publish('foo', function (err)
        {
            expect(err.message).to.equal('buffer too small');
            cb();
        }).end('bar');
    });

    with_mqs(1, 'client should emit error event when mux errors',
    function (mqs, cb)
    {
        mqs[0].client.on('error', function (err)
        {
            expect(err.message).to.equal('test error');
            cb();
        });
        mqs[0].client.mux.emit('error', new Error('test error'));
    });

    with_mqs(1, 'server should emit error event when mux errors',
    function (mqs, cb)
    {
        mqs[0].server.on('error', function (err)
        {
            expect(err.message).to.equal('test error');
            cb();
        });
        mqs[0].server.mux.emit('error', new Error('test error'));
    });

    with_mqs(1, 'client should throw exception when called after stream finishes', function (mqs, cb, end)
    {
        end(function ()
        {
            mqs[0].client.subscribe('foo', function ()
            {
                cb(new Error('should not be called'));
            }, function (err)
            {
                expect(err.message).to.equal('finished');
                mqs[0].client.unsubscribe(function (err)
                {
                    expect(err.message).to.equal('finished');
                    mqs[0].client.publish('foo', function (err)
                    {
                        expect(err.message).to.equal('finished');
                        cb(); 
                    }).end('bar');
                });
            });
        });
    });

    with_mqs(1, 'should not send expiry time if not requested',
    function (mqs, cb)
    {
        mqs[0].client.subscribe('foo', function (s, info)
        {
            expect(info.single).to.equal(false);
            expect(info.topic).to.equal('foo');
            expect(info.expires).to.equal(undefined);

            read_all(s, function (v)
            {
                expect(v.toString()).to.equal('bar');
                cb();
            });
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.publish('foo', function (err)
            {
                if (err) { return cb(err); }
            }).end('bar');
        });
    }, it, null);

    with_mqs(1, 'should support omitting callbacks', function (mqs, cb)
    {
        mqs[0].server.on('unsubscribe_all_requested', function (done)
        {
            this.unsubscribe(function (err)
            {
                if (err) { return cb(err); }
                done();
                cb();
            });
        });

        mqs[0].server.on('publish_requested', function (topic, duplex, options, done)
        {
            expect(topic).to.equal('foo');

            mqs[0].client.subscribe('foo', function (s, info)
            {
                expect(info.topic).to.equal('foo');

                read_all(s, function (v)
                {
                    expect(v.toString()).to.equal('bar');
                    mqs[0].client.unsubscribe();
                });
            });

            duplex.pipe(this.fsq.publish(topic, options, done));
        });

        mqs[0].server.on('unsubscribe_requested', function (topic, done)
        {
            expect(topic).to.equal('foo');
            this.unsubscribe(topic, function (err)
            {
                if (err) { return cb(err); }
                // wait for client to get response
                mqs[0].client.mux.once('handshake', function ()
                {
                    process.nextTick(function ()
                    {
                        mqs[0].client.publish('foo').end('bar');
                    });
                });
                done();
            });
        });

        mqs[0].server.once('subscribe_requested', function (topic, done)
        {
            expect(topic).to.equal('foo');
            this.subscribe(topic, function (err)
            {
                if (err) { return cb(err); }
                // wait for client to get response
                mqs[0].client.mux.once('handshake', function ()
                {
                    process.nextTick(function ()
                    {
                        mqs[0].client.unsubscribe('foo');
                    });
                });
                done();
            });
        });

        mqs[0].client.subscribe('foo', function (s)
        {
            cb(new Error('should not be called'));
        });
    });

    with_mqs(1, 'should warn about errors when callbacks are omitted', function (mqs, cb)
    {
        mqs[0].client.subscribe('bar', function ()
        {
            cb(new Error('should not be called'));
        }, function (err)
        {
            if (err) { return cb(err); }

            var server_warnings = [],
                client_warnings = [];

            function error()
            {
                arguments[arguments.length - 1](new Error('dummy'));
            }

            function check()
            {
                if ((server_warnings.length > 5) ||
                    (client_warnings.length > 4))
                {
                    return cb(new Error('called too many times'));
                }

                if ((server_warnings.length === 5) &&
                    (client_warnings.length === 4))
                {
                    expect(server_warnings).to.eql([
                        'dummy', 'dummy', 'dummy', 'dummy', 'unexpected data']);
                    expect(client_warnings).to.eql([
                        'server error', 'server error', 'server error', 'server error']);
                    cb();
                }
            }

            mqs[0].server.on('warning', function (err)
            {
                server_warnings.push(err.message);
                check();
            });

            mqs[0].client.on('warning', function (err)
            {
                client_warnings.push(err.message);
                check();
            });

            mqs[0].server.on('publish_requested', error);
            mqs[0].server.on('subscribe_requested', error);
            mqs[0].server.on('unsubscribe_requested', error);
            mqs[0].server.on('unsubscribe_all_requested', error);

            mqs[0].client.subscribe('foo', function ()
            {
                cb(new Error('should not be called'));
            });

            mqs[0].client.unsubscribe('bar');
            mqs[0].client.unsubscribe();

            mqs[0].client.publish('bar').end('bar');
        });
    });

    with_mqs(1, 'should not ask server to unsubscribe all if there are no subscriptions', function (mqs, cb)
    {
        mqs[0].server.on('unsubscribe_all_requested', function ()
        {
            cb(new Error('should not be called'));
        });

        mqs[0].client.unsubscribe(cb);
    });

    with_mqs(1, 'should not ask server to unsubscribe if there are no subscriptions for a topic', function (mqs, cb)
    {
        mqs[0].server.on('unsubscribe_requested', function ()
        {
            cb(new Error('should not be called'));
        });

        mqs[0].client.subscribe('foo', function ()
        {
            cb(new Error('should not be called'));
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.unsubscribe('bar', undefined, cb);
        });
    });

    with_mqs(1, 'should not ask server to unsubscribe if handler has not been subscribed', function (mqs, cb)
    {
        function handler1() { }
        function handler2() { }

        mqs[0].server.on('unsubscribe_requested', function ()
        {
            cb(new Error('should not be called'));
        });

        mqs[0].client.subscribe('foo', handler1, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.unsubscribe('foo', handler2, cb);
        });
    });

    with_mqs(1, 'should emit full event when client handshakes are backed up', function (mqs, cb, end)
    {
        var orig_write = mqs[0].client_stream._write,
            the_chunk,
            the_encoding,
            the_callback,
            count_complete = 0,
            count_incomplete = 0,
            count_pub_error = 0,
            count_sub_error = 0,
            count_unsub_error = 0,
            count_cons_error = 0,
            ended = false,
            full_called = false;

        function check_end()
        {
            if (ended &&
                count_pub_error === 2993 &&
                count_sub_error === 1 &&
                count_unsub_error === 1 &&
                count_cons_error === 1)
            {
                cb();
            }
            else if (count_pub_error > 2993 ||
                     count_sub_error > 1 ||
                     count_unsub_error > 1 ||
                     count_cons_error > 1)
            {
                cb(new Error('called too many times'));
            }
        }

        mqs[0].client_stream._write = function (chunk, encoding, callback)
        {
            the_chunk = chunk;
            the_encoding = encoding;
            the_callback = callback;
        };

        // number will change if bpmux handhsake buffer size changes

        mqs[0].client.on('backoff', function ()
        {
            expect(count_complete).to.equal(2992);
            expect(count_incomplete).to.equal(0); // only counted below
            full_called = true;
        });

        function sent(complete)
        {
            if (complete)
            {
                count_complete += 1;
            }
            else
            {
                count_incomplete += 1;
            }
            if ((count_complete + count_incomplete) == 2993)
            {
                expect(count_complete).to.equal(2992);
                expect(count_incomplete).to.equal(1);
                expect(full_called).to.equal(true);
                mqs[0].client.subscribe('foo', function () {}, function (err)
                {
                    expect(err.message).to.equal('ended before handshaken');
                    count_sub_error += 1;
                    check_end();
                });
                mqs[0].client._subs.set('foo', new Set([function () {}]));
                mqs[0].client.unsubscribe(function (err)
                {
                    expect(err.message).to.equal('ended before handshaken');
                    count_unsub_error += 1;
                    check_end();
                });
                new MQlobberClient(mqs[0].client_stream).on('error', function (err)
                {
                    expect(err.message).to.equal('ended before handshaken');
                    count_cons_error += 1;
                    check_end();
                });
                mqs[0].server.subscribe('foo', function (err)
                {
                    if (err) { return cb(err); }
                    mqs[0].client_stream._write = orig_write;
                    mqs[0].client_stream._write(the_chunk, the_encoding, the_callback);
                    end(function ()
                    {
                        ended = true;
                        check_end();
                    });
                });
            }
            else if ((count_complete + count_incomplete) > 2993)
            {
                cb(new Error('called too many times'));
            }
        }

        function onpub(err)
        {
            expect(err.message).to.equal('ended before handshaken');
            count_pub_error += 1;
            check_end();
        }

        for (var i=0; i < 2993; i += 1)
        {
            var duplex = mqs[0].client.publish('foo', onpub);
            duplex.on('handshake_sent', sent);
            duplex.end('bar');
        }
    });

    with_mqs(1, 'should publish and receive work on single stream',
    function (mqs, cb)
    {
        mqs[0].client.subscribe('foo', function (s, info, done)
        {
            mqs[0].server.on('ack', function ()
            {
                cb();
            });

            expect(info.single).to.equal(true);
            expect(info.topic).to.equal('foo');

            var now = Date.now(), expires = info.expires * 1000;

            expect(expires).to.be.above(now);
            expect(expires).to.be.at.most(now + timeout * 2 * 1000);

            read_all(s, function (v)
            {
                expect(v.toString()).to.equal('bar');
                done();
            });
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.publish('foo', { single: true }, function (err)
            {
                if (err) { return cb(err); }
            }).end('bar');
        });
    });

    with_mqs(1, 'should publish and receive work using one handler on single stream',
    function (mqs, cb)
    {
        var calls = 0, data = '';

        function check(s, info, done)
        {
            expect(info.single).to.equal(true);
            expect(info.topic).to.equal('foo');

            var d = new stream.PassThrough();
            s.pipe(d);

            read_all(d, function (v)
            {
                var str = v.toString();
                expect(str).to.equal('bar');
                data += str;
                done();
            });

            calls += 1;
        }

        mqs[0].client.subscribe('foo', function (s, info, done)
        {
            check(s, info, done);
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.subscribe('foo', function (s, info, done)
            {
                check(s, info, done);
            }, function (err)
            {
                if (err) { return cb(err); }
                mqs[0].client.publish('foo', { single: true }, function (err)
                {
                    if (err) { return cb(err); }
                    setTimeout(function ()
                    {
                        expect(calls).to.equal(1);
                        expect(data).to.equal('bar');
                        cb();
                    }, 2000);
                }).end('bar');
            });
        });
    });

    with_mqs(3, 'should publish and receive work using one handler on multiple streams',
    function (mqs, cb)
    {
        var calls = 0, data = '';

        function check(s, info, done)
        {
            expect(info.single).to.equal(true);
            expect(info.topic).to.equal('foo');

            var d = new stream.PassThrough();
            s.pipe(d);

            read_all(d, function (v)
            {
                var str = v.toString();
                expect(str).to.equal('bar');
                data += str;
                done();
            });

            calls += 1;
        }

        mqs[0].client.subscribe('foo', function (s, info, done)
        {
            check(s, info, done);
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[1].client.subscribe('foo', function (s, info, done)
            {
                check(s, info, done);
            }, function (err)
            {
                if (err) { return cb(err); }
                mqs[2].client.publish('foo', { single: true }, function (err)
                {
                    if (err) { return cb(err); }
                    setTimeout(function ()
                    {
                        expect(calls).to.equal(1);
                        expect(data).to.equal('bar');
                        cb();
                    }, 2000);
                }).end('bar');
            });
        });
    });

    with_mqs(1, 'client should write warning to console if no event listeners are registered', function (mqs, cb)
    {
        this.sinon.stub(console, 'error');

        setTimeout(function ()
        {
            expect(console.error.calledOnce).to.equal(true);
            expect(console.error.calledWith(new Error('buffer too small'))).to.equal(true);
            cb();
        }.bind(this), 1000);

        mqs[0].server.mux.multiplex();
    }, it, { sinon: true });

    with_mqs(1, 'server should warn about empty handshake data', function (mqs, cb)
    {
        mqs[0].server.on('warning', function (err, duplex)
        {
            expect(err.message).to.equal('buffer too small');
            expect(duplex).to.be.an.instanceof(stream.Duplex);
            cb();
        });

        mqs[0].client.mux.multiplex();
    });

    with_mqs(1, 'server should warn about short handshake data (no flags)', function (mqs, cb)
    {
        mqs[0].server.on('warning', function (err, duplex)
        {
            expect(err.message).to.equal('buffer too small');
            expect(duplex).to.be.an.instanceof(stream.Duplex);
            cb();
        });

        mqs[0].client.mux.multiplex(
        {
            handshake_data: new Buffer([3])
        });
    });

    with_mqs(1, 'server should warn about short handshake data (no ttl)', function (mqs, cb)
    {
        mqs[0].server.on('warning', function (err, duplex)
        {
            expect(err.message).to.equal('buffer too small');
            expect(duplex).to.be.an.instanceof(stream.Duplex);
            cb();
        });

        mqs[0].client.mux.multiplex(
        {
            handshake_data: new Buffer([3, 2])
        });
    });

    with_mqs(1, 'should warn about unknown operation type', function (mqs, cb)
    {
        mqs[0].server.on('warning', function (err, duplex)
        {
            expect(err.message).to.equal('unknown type: 100');
            expect(duplex).to.be.an.instanceof(stream.Duplex);
            cb();
        });

        mqs[0].client.mux.multiplex(
        {
            handshake_data: new Buffer([100])
        });
    });

    with_mqs(1, 'server should write warning to console if no event listeners are registered', function (mqs, cb)
    {
        this.sinon.stub(console, 'error');

        setTimeout(function ()
        {
            expect(console.error.calledOnce).to.equal(true);
            expect(console.error.calledWith(new Error('buffer too small'))).to.equal(true);
            cb();
        }.bind(this), 1000);

        mqs[0].client.mux.multiplex();
    }, it, { sinon: true });

    with_mqs(1, 'should emit full event when server handshakes are backed up', function (mqs, cb)
    {
        var orig_write = mqs[0].server_stream._write,
            the_chunk,
            the_encoding,
            the_callback,
            count_complete = 0,
            count_incomplete = 0,
            full_called = false;

        mqs[0].server.on('warning', function (err)
        {
            expect(err.message).to.equal('unexpected data');
        });

        mqs[0].server_stream._write = function (chunk, encoding, callback)
        {
            the_chunk = chunk;
            the_encoding = encoding;
            the_callback = callback;
        };

        // number will change if bpmux handhsake buffer size changes

        mqs[0].server.on('backoff', function ()
        {
            expect(count_complete).to.equal(2517);
            expect(count_incomplete).to.equal(0); // only counted below
            full_called = true;
        });

        function sent(complete)
        {
            if (complete)
            {
                count_complete += 1;
            }
            else
            {
                count_incomplete += 1;
            }
            if ((count_complete + count_incomplete) == 2518)
            {
                expect(count_complete).to.equal(2517);
                expect(count_incomplete).to.equal(1);
                expect(full_called).to.equal(true);
                mqs[0].server_stream._write = orig_write;
                mqs[0].server_stream._write(the_chunk, the_encoding, the_callback);
                cb();
            }
        }

        mqs[0].server.on('publish_requested', function (topic, duplex, options, done)
        {
            duplex.on('handshake_sent', sent);
            done();
        });

        for (var i=0; i < 2518; i += 1)
        {
            mqs[0].client.publish('foo').end('bar');
        }
    });

    with_mqs(1, 'should emit a message event when fsq gives it a message', function (mqs, cb)
    {
        mqs[0].server.on('message', function (data, info, multiplex)
        {
            expect(info.single).to.equal(false);
            expect(info.topic).to.equal('foo');

            var now = Date.now();

            expect(info.expires).to.be.above(now);
            expect(info.expires).to.be.at.most(now + timeout * 1000);

            expect(multiplex).to.be.an.instanceof(Function);

            read_all(data, function (v)
            {
                expect(v.toString()).to.equal('bar');
                cb();
            });
        });

        mqs[0].client.subscribe('foo', function ()
        {
            cb(new Error('should not be called'));
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.publish('foo').end('bar');
        });
    });

    with_mqs(1, 'should be able to ignore a message received from fsq',
    function (mqs, cb)
    {
        mqs[0].server.on('message', function (data, info, multiplex, done)
        {
            expect(info.single).to.equal(false);
            expect(info.topic).to.equal('foo');

            var now = Date.now();

            expect(info.expires).to.be.above(now);
            expect(info.expires).to.be.at.most(now + timeout * 1000);

            expect(done).to.be.an.instanceof(Function);

            var msg1, msg2;

            this.fsq.on('warning', function (err)
            {
                msg1 = err.message;
            });

            data.on('error', function (err)
            {
                msg2 = err.message;
            });

            done(new Error('dummy'), function ()
            {
                expect(msg1).to.equal('dummy');
                expect(msg2).to.equal('dummy');

                // check stream was ended
                read_all(data, function (v)
                {
                    expect(v.toString()).to.equal('');
                    cb();
                });
            });
        });

        mqs[0].client.subscribe('foo', function ()
        {
            cb(new Error('should not be called'));
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.publish('foo').end('bar');
        });
    });

    with_mqs(1, 'should be able to do server callback after multiplexing',
    function (mqs, cb)
    {
        var msg1, msg2, msg3;

        mqs[0].server.on('warning', function (err)
        {
            msg1 = err.message;
        });

        mqs[0].server.fsq.on('warning', function (err)
        {
            msg2 = err.message;
        });

        mqs[0].server.on('message', function (data, info, multiplex, done)
        {
            expect(info.single).to.equal(true);
            expect(info.topic).to.equal('foo');

            var now = Date.now();

            expect(info.expires).to.be.above(now);
            expect(info.expires).to.be.at.most(now + timeout * 2 * 1000);

            expect(done).to.be.an.instanceof(Function);

            data.on('error', function (err)
            {
                msg3 = err.message;
            });

            data.pipe(multiplex());

            done(new Error('dummy2'));
        });

        mqs[0].client.on('warning', function (err)
        {
            expect(err.message).to.equal('dummy');

            expect(msg1).to.equal('dummy2');
            expect(msg2).to.equal('dummy2');
            expect(msg3).to.equal('dummy2');

            if (cb)
            {
                cb();
            }
            cb = null;
        });

        mqs[0].client.subscribe('foo', function (s, info, done)
        {
            expect(info.single).to.equal(true);
            expect(info.topic).to.equal('foo');

            var now = Date.now(), expires = info.expires * 1000;

            expect(expires).to.be.above(now);
            expect(expires).to.be.at.most(now + timeout * 2 * 1000);

            read_all(s, function (v)
            {
                expect(v.toString()).to.equal('');
                done(new Error('dummy'));
            });
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.publish('foo', { single: true }).end('bar');
        });
    });

    with_mqs(1, 'should be able to do client callback after multiplexing',
    function (mqs, cb)
    {
        var msgs, server_done;

        mqs[0].server.on('warning', function (err)
        {
            expect(err.message).to.equal('client error');
            msgs.push('server warning');
        });

        mqs[0].server.fsq.once('warning', function (err)
        {
            expect(err.message).to.equal('client error');
            msgs.push('fsq warning');
        });

        mqs[0].server.on('message', function (data, info, multiplex, done)
        {
            expect(info.single).to.equal(true);
            expect(info.topic).to.equal('foo');

            var now = Date.now();

            expect(info.expires).to.be.above(now);
            expect(info.expires).to.be.at.most(now + timeout * 2 * 1000);

            expect(done).to.be.an.instanceof(Function);

            data.on('error', function (err)
            {
                expect(err.message).to.equal('client error');
                msgs.push('data error');
            });

            server_done = done;

            data.pipe(multiplex());
        });

        mqs[0].client.on('warning', function (err)
        {
            expect(err.message).to.equal('dummy');
            msgs.push('client warning');
        });

        mqs[0].client.subscribe('foo', function (s, info, done)
        {
            expect(info.single).to.equal(true);
            expect(info.topic).to.equal('foo');

            var now = Date.now(), expires = info.expires * 1000;

            expect(expires).to.be.above(now);
            expect(expires).to.be.at.most(now + timeout * 2 * 1000);

            msgs = [];
            done(new Error('dummy'));

            read_all(s, function (v)
            {
                expect(msgs).to.eql(['client warning',
                                     'fsq warning',
                                     'server warning',
                                     'data error']);

                // depending how soon the error gets to the server,
                // the data may already have been sent
                expect(v.toString()).to.be.oneOf(['', 'bar']);

                mqs[0].server.fsq.once('warning', function (err)
                {
                    expect(err.message).to.equal('dummy');

                    if (cb)
                    {
                        cb();
                    }
                    cb = null;
                });

                // check server_done can be called too
                server_done(new Error('dummy'));
            });
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.publish('foo', { single: true }).end('bar');
        });
    });

    with_mqs(1, 'should publish and receive work with ttl',
    function (mqs, cb)
    {
        mqs[0].client.subscribe('foo', function (s, info, done)
        {
            mqs[0].server.on('ack', function ()
            {
                cb();
            });

            expect(info.single).to.equal(true);
            expect(info.topic).to.equal('foo');

            var now = Date.now(), expires = info.expires * 1000;

            expect(expires).to.be.above(now);
            expect(expires).to.be.at.most(now + timeout * 1000);

            read_all(s, function (v)
            {
                expect(v.toString()).to.equal('bar');
                done();
            });
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.publish('foo',
            {
                single: true,
                ttl: timeout
            }, function (err)
            {
                if (err) { return cb(err); }
            }).end('bar');
        });
    });

    with_mqs(1, 'server should pass on fsq errors in its subscribe method',
    function (mqs, cb)
    {
        mqs[0].server.fsq.subscribe = function (topic, handler, options, cb)
        {
            cb(new Error('test'));
        };

        mqs[0].server.subscribe('foo', function (err)
        {
            expect(err.message).to.equal('test');
            cb();
        });
    });
    
    with_mqs(1, 'server should warn about fsq errors in its subscribe method',
    function (mqs, cb)
    {
        this.sinon.stub(console, 'error');

        mqs[0].server.fsq.subscribe = function (topic, handler, options, cb)
        {
            cb(new Error('test'));
        };

        setTimeout(function ()
        {
            expect(console.error.calledOnce).to.equal(true);
            expect(console.error.calledWith(new Error('test'))).to.equal(true);
            cb();
        }.bind(this), 1000);

        mqs[0].server.subscribe('foo');
    }, it, { sinon: true });

    with_mqs(1, 'server should not warn if there are no fsq errors in its subscribe method',
    function (mqs, cb)
    {
        this.sinon.stub(console, 'error');

        setTimeout(function ()
        {
            expect(console.error.called).to.equal(false);
            cb();
        }.bind(this), 1000);

        mqs[0].server.subscribe('foo');
    }, it, { sinon: true });

    with_mqs(1, 'server should pass on fsq errors in its unsubscribe(all) method',
    function (mqs, cb)
    {
        var orig_unsubscribe = mqs[0].server.fsq.unsubscribe;

        mqs[0].server.fsq.unsubscribe = function (topic, handler, cb)
        {
            cb(new Error('test'));
        };

        mqs[0].server.subscribe('foo', function (err)
        {
            if (err) { return cb(err); }

            mqs[0].server.unsubscribe(function (err)
            {
                expect(err.message).to.equal('test');
                mqs[0].server.fsq.unsubscribe = orig_unsubscribe;
                cb();
            });
        });
    });
 
    with_mqs(1, 'server should pass on fsq errors in its unsubscribe(topic) method',
    function (mqs, cb)
    {
        var orig_unsubscribe = mqs[0].server.fsq.unsubscribe;

        mqs[0].server.fsq.unsubscribe = function (topic, handler, cb)
        {
            cb(new Error('test'));
        };

        mqs[0].server.subscribe('foo', function (err)
        {
            if (err) { return cb(err); }

            mqs[0].server.unsubscribe('foo', function (err)
            {
                expect(err.message).to.equal('test');
                mqs[0].server.fsq.unsubscribe = orig_unsubscribe;
                cb();
            });
        });
    });

    with_mqs(1, 'server should warn about fsq errors in its unsubscribe method',
    function (mqs, cb)
    {
        this.sinon.stub(console, 'error');

        var orig_unsubscribe = mqs[0].server.fsq.unsubscribe;

        mqs[0].server.fsq.unsubscribe = function (topic, handler, cb)
        {
            cb(new Error('test'));
        };

        mqs[0].server.subscribe('foo', function (err)
        {
            if (err) { return cb(err); }

            setTimeout(function ()
            {
                expect(console.error.calledOnce).to.equal(true);
                expect(console.error.calledWith(new Error('test'))).to.equal(true);
                mqs[0].server.fsq.unsubscribe = orig_unsubscribe;
                cb();
            }.bind(this), 1000);

            mqs[0].server.unsubscribe();
        });
    }, it, { sinon: true });

    with_mqs(1, 'server should callback without error when unsubscribing from topic not subscribed to',
    function (mqs, cb)
    {
        expect(mqs[0].server._subs.has('foo')).to.equal(false);
        mqs[0].server.unsubscribe('foo', cb);
    });

    function rabbitmq_topic_tests3(d, topics_per_mq, expected, rounds, f)
    {
        var num_mqs = Math.ceil(rabbitmq_bindings.test_bindings.length / topics_per_mq),
            expected2 = {},
            expected_single = new Map();

        for (var entry of expected)
        {
            expected2[entry[0]] = [];
            expected_single.set(entry[0], new Set());

            for (var t of entry[1])
            {
                var t2 = 't' + (Math.floor((parseInt(t.substr(1), 10) - 1) / topics_per_mq) + 1);

                for (var i = 0; i < rounds; i += 1)
                {
                    expected2[entry[0]].push(t2);
                }

                expected_single.get(entry[0]).add(t2);
            }
        }

        describe('rabbitmq topic tests (' + d + ', topics_per_mq=' + topics_per_mq + ', rounds=' + rounds + ')',
        function ()
        {
            with_mqs(num_mqs, 'should match topics correctly',
            function (mqs, cb)
            {
                var results = {},
                    results_single = new Map(),
                    total = 0,
                    count = 0,
                    count_single = 0;

                for (var i = 0; i < expected.length; i += 1)
                {
                    total += expected[i][1].length * rounds;
                }

                async.times(rabbitmq_bindings.test_bindings.length, function (i, cb2)
                {
                    var n = Math.floor(i / topics_per_mq);

                    function handler(s, info, done)
                    {
                        s.setMaxListeners(0);

                        var pthru = new stream.PassThrough();
                        s.pipe(pthru);

                        read_all(pthru, function (v)
                        {
                            expect(v.toString()).to.equal(info.topic);

                            done();

                            if (info.single)
                            {
                                if (!results_single.has(info.topic))
                                {
                                    results_single.set(info.topic, new Set());
                                }

                                results_single.get(info.topic).add('t' + (n + 1));
                                count_single += 1;
                            }
                            else
                            {
                                if (results[info.topic] === undefined)
                                {
                                    results[info.topic] = [];
                                }

                                results[info.topic].push('t' + (n + 1));
                                count += 1;
                            }

                            if ((count === total) &&
                                (count_single === expected_single.size * rounds))
                            {
                                for (var t in results)
                                {
                                    if (results.hasOwnProperty(t))
                                    {
                                        results[t].sort(topic_sort);
                                    }
                                }

                                expect(results).to.eql(expected2);

                                expect(results_single.size).to.equal(expected_single.size);

                                results_single.forEach(function (v, k)
                                {
                                    v.forEach(function (w)
                                    {
                                        expect(expected_single.get(k).has(w)).to.equal(true);
                                    });
                                });

                                setTimeout(cb, 2000);
                            }
                            else if ((count > total) ||
                                     (count_single > expected_single.size * rounds))
                            {
                                cb(new Error('too many messages'));
                            }
                        });
                    }

                    mqs[n].client.subscribe(rabbitmq_bindings.test_bindings[i][0], handler, function (err)
                    {
                        cb2(err, handler);
                    });
                }, function (err, handlers)
                {
                    if (err) { return cb(err); }

                    function publish(err)
                    {
                        if (err) { return cb(err); }

                        async.timesLimit(rounds, num_mqs * 5, function (x, cb3)
                        {
                            async.times(expected.length, function (i, cb4)
                            {
                                var entry = expected[i];

                                async.parallel(
                                [
                                    function (cb5)
                                    {
                                        mqs[i % num_mqs].client.publish(
                                                entry[0],
                                                cb5).end(entry[0]);
                                    },
                                    function (cb5)
                                    {
                                        mqs[i % num_mqs].client.publish(
                                                entry[0],
                                                { single: true },
                                                cb5).end(entry[0]);
                                    }
                                ], cb4);
                            }, cb3);
                        }, function (err)
                        {
                            if (err) { return cb(err); }

                            if (total === 0)
                            {
                                setTimeout(function ()
                                {
                                    expect(count).to.equal(0);
                                    expect(count_single).to.equal(0);
                                    cb();
                                }, 30 * 1000);
                            }
                        });
                    }
                    
                    if (f)
                    {
                        f(topics_per_mq, mqs, handlers, publish);
                    }
                    else
                    {
                        publish();
                    }
                });
            });
        });
    }

    function rabbitmq_topic_tests2(d, expected, rounds, f)
    {
        rabbitmq_topic_tests3(d, 1, expected, rounds, f);
        rabbitmq_topic_tests3(d, 2, expected, rounds, f);
        rabbitmq_topic_tests3(d, 10, expected, rounds, f);
        rabbitmq_topic_tests3(d, 26, expected, rounds, f);
    }

    function rabbitmq_topic_tests(d, expected, f)
    {
        rabbitmq_topic_tests2(d, expected, 1, f);
        rabbitmq_topic_tests2(d, expected, 10, f);
        rabbitmq_topic_tests2(d, expected, 50, f);
    }

    rabbitmq_topic_tests('before remove', rabbitmq_bindings.expected_results_before_remove);

    rabbitmq_topic_tests('after remove', rabbitmq_bindings.expected_results_after_remove, function (topics_per_mq, mqs, handlers, cb)
    {
        async.each(rabbitmq_bindings.bindings_to_remove, function (i, cb)
        {
            i = i - 1;
            var n = Math.floor(i / topics_per_mq);
            mqs[n].client.unsubscribe(rabbitmq_bindings.test_bindings[i][0], handlers[i], cb);
        }, cb);
    });

    rabbitmq_topic_tests('after remove all', rabbitmq_bindings.expected_results_after_remove_all, function (topics_per_mq, mqs, handlers, cb)
    {
        async.each(rabbitmq_bindings.bindings_to_remove, function (i, cb)
        {
            i = i - 1;
            async.each(mqs, function (mq, cb)
            {
                mq.client.unsubscribe(rabbitmq_bindings.test_bindings[i][0], undefined, cb);
            }, cb);
        }, cb);
    });

    rabbitmq_topic_tests('after clear', rabbitmq_bindings.expected_results_after_clear, function (topics_per_mq, mqs, handlers, cb)
    {
        async.each(mqs, function (mq, cb)
        {
            mq.client.unsubscribe(cb);
        }, cb);
    });

    // https://github.com/nodejs/node/pull/7292 isn't on 0.12
    if ((type != 'in-memory') || (parseFloat(process.versions.node) > 0.12))
    {
        with_mqs(2, 'server should support setting custom data on message info and stream',
        function (mqs, cb)
        {
            var message0_called = false,
                message1_called = false,
                laggard0_called = false,
                laggard1_called = false,
                buf = new Buffer(100 * 1024);

            buf.fill('a');

            function check(msg_stream, info, duplex)
            {
                expect(info.topic).to.equal('foo');
                expect(msg_stream.fastest_writable === undefined).to.equal(info.count === 0 ? true : false);
                info.count += 1;

                if (!msg_stream.fastest_writable)
                {
                    msg_stream.fastest_writable = new FastestWritable(
                    {
                        emit_laggard: true
                    });
                    msg_stream.pipe(msg_stream.fastest_writable);
                }

                msg_stream.fastest_writable.add_peer(duplex);

                if (info.count === info.num_handlers)
                {
                    // make fastest_writable enter waiting state
                    msg_stream.fastest_writable.write(buf);
                }
            }

            mqs[0].server.on('message', function (msg_stream, info, multiplex)
            {
                expect(message0_called).to.equal(false);
                message0_called = true;
                var duplex = multiplex();
                check(msg_stream, info, duplex);
                duplex.on('laggard', function ()
                {
                    laggard0_called = true;
                });
            });

            mqs[1].server.on('message', function (msg_stream, info, multiplex)
            {
                expect(message1_called).to.equal(false);
                message1_called = true;
                var null_stream = new NullStream();
                null_stream.on('laggard', function ()
                {
                    laggard1_called = true;
                });
                check(msg_stream, info, null_stream);
            });

            mqs[0].client.subscribe('foo', function (s, info)
            {
                expect(info.topic).to.equal('foo');
                read_all(s, function (v)
                {
                    expect(v.toString()).to.equal(buf.toString() + 'bar');
                    setTimeout(function ()
                    {
                        expect(laggard0_called).to.equal(false);
                        expect(laggard1_called).to.equal(true);
                        cb();
                    }, 2000);
                });
            }, function (err)
            {
                if (err) { return cb(err); }
                mqs[1].client.subscribe('#', function (s, info)
                {
                    cb(new Error('should not be called'));
                }, function (err)
                {
                    if (err) { return cb(err); }
                    mqs[0].client.publish('foo').end('bar');
                });
            });
        }, it,
        {
            filter: function (info, handlers, cb)
            {
                info.num_handlers = handlers.size;
                info.count = 0;
                cb(null, true, handlers);
            }
        });

        with_mqs(1, 'server should support delaying message until all streams are under high-water mark',
        function (mqs, cb)
        {
            mqs[0].client.subscribe('bar', function (s)
            {
                mqs[0].server.bar_s = s;
                // don't read so server is backed up
                mqs[0].client.publish('foo', function (err)
                {
                    if (err) { return cb(err); }
                }).end('hello');
            }, function (err)
            {
                if (err) { return cb(err); }
                mqs[0].client.subscribe('foo', function (s)
                {
                    read_all(s, function (v)
                    {
                        expect(v.toString()).to.equal('hello');
                        cb();
                    });
                }, function (err)
                {
                    if (err) { return cb(err); }
                    mqs[0].client.publish('bar', function (err)
                    {
                        if (err) { return cb(err); }
                    }).end(new Buffer(128 * 1024));
                });
            });
        }, it,
        {
            handler_concurrency: 1,

            filter: function (info, handlers, cb)
            {
                if (info.topic === 'bar')
                {
                    return cb(null, true, handlers);
                }

                for (var h of handlers)
                {
                    if (h.mqlobber_server)
                    {
                        for (var d of h.mqlobber_server.mux.duplexes.values())
                        {
                            if (d._writableState.length >=
                                d._writableState.highWaterMark)
                            {
                                /* drain 'bar' stream on client */
                                var bar_s = h.mqlobber_server.bar_s;
                                if (bar_s)
                                {
                                    read_all(bar_s);
                                    h.mqlobber_server.bar_s = null;
                                }

                                return cb(null, false);
                            }
                        }
                    }
                }

                cb(null, true, handlers);
            }
        });
    }

    with_mqs(1, 'server should warn about unexpected data', function (mqs, cb)
    {
        var duplex, warned = false, got_message = false;

        function check()
        {
            if (warned && got_message)
            {
                cb();
            }
        }

        mqs[0].server.on('warning', function (err, obj)
        {
            expect(err.message).to.equal('unexpected data');
            expect(obj).to.be.an.instanceof(stream.Duplex);
            expect(obj).to.equal(duplex);
            warned = true;
            check();
        });

        mqs[0].server.on('message', function (data, info, multiplex)
        {
            duplex = multiplex();
            expect(duplex).to.be.an.instanceof(stream.Duplex);
            data.pipe(duplex);
        });

        mqs[0].client.subscribe('foo', function (s)
        {
            read_all(s, function ()
            {
                got_message = true;
                check();
            });
        }, function (err)
        {
            if (err) { return cb(err); }

            var listeners = mqs[0].client.mux.listeners('handshake');
            listeners.unshift(function (duplex, hdata, delay)
            {
                if (!delay)
                {
                    return;
                }

                duplex.write('a');
            });

            mqs[0].client.mux.removeAllListeners('handshake');

            for (var l of listeners)
            {
                mqs[0].client.mux.on('handshake', l);
            }

            mqs[0].client.publish('foo').end('bar');
        });
    });

    with_mqs(1, 'client should warn about unexpected data', function (mqs, cb)
    {
        var count = 0;

        mqs[0].client.on('warning', function (err, obj)
        {
            expect(err.message).to.equal('unexpected data');
            expect(obj).to.be.an.instanceof(stream.Duplex);
            count += 1;
            if (count === 3)
            {
                cb();
            } else if (count > 3)
            {
                cb(new Error('called too many times'));
            }
        });

        var listeners = mqs[0].server.mux.listeners('handshake');
        listeners.unshift(function (duplex, hdata, delay)
        {
            if (!delay)
            {
                return;
            }

            duplex.write('a');
        });

        mqs[0].server.mux.removeAllListeners('handshake');

        for (var l of listeners)
        {
            mqs[0].server.mux.on('handshake', l);
        }

        mqs[0].client.subscribe('foo', function ()
        {
            cb(new Error('should not be called'));
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.unsubscribe(function (err)
            {
                if (err) { return cb(err); }
                mqs[0].client.publish('foo', function (err)
                {
                    if (err) { return cb(err); }
                }).end('bar');
            });
        });
    });

    with_mqs(1, 'should clean up duplexes', function (mqs, cb)
    {
        mqs[0].client.subscribe('foo', function (s)
        {
            read_all(s, function (v)
            {
                expect(v.toString()).to.equal('bar');
                mqs[0].client.unsubscribe('foo', undefined, function (err)
                {
                    if (err) { return cb(err); }
                    process.nextTick(function ()
                    {
                        expect(mqs[0].client.mux.duplexes.size).to.equal(0);
                        expect(mqs[0].server.mux.duplexes.size).to.equal(0);
                        cb();
                    });
                });
            });
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.publish('foo', function (err)
            {
                if (err) { return cb(err); }
            }).end('bar');
        });
    });

    with_mqs(1, 'should tell server when processing work is done',
    function (mqs, cb)
    {
        mqs[0].client.subscribe('foo', function (s, info, done)
        {
            mqs[0].server.mux.on('handshake', function (duplex, hdata, delay)
            {
                expect(delay).to.equal(null);
                expect(hdata).to.eql(new Buffer([0]));
                cb();
            });

            done();
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.publish('foo', { single: true }, function (err)
            {
                if (err) { return cb(err);  }
            }).end('bar');
        });
    });

    with_mqs(1, 'should tell server when processing work errors',
    function (mqs, cb)
    {
        mqs[0].server.fsq.on('warning', function warning(err)
        {
            expect(err.message).to.equal('client error');

            if (count === 3)
            {
                this.removeListener('warning', warning);
                cb();
            }
        });

        mqs[0].client.on('warning', function (err)
        {
            expect(err.message).to.equal('dummy');
        });

        var count = 0;

        mqs[0].client.subscribe('foo', function (s, info, done)
        {
            mqs[0].server.mux.once('handshake', function (duplex, hdata, delay)
            {
                expect(delay).to.equal(null);
                expect(hdata).to.eql(new Buffer([1]));
                count += 1;
            });

            done(new Error('dummy'));
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.publish('foo', { single: true }, function (err)
            {
                if (err) { return cb(err);  }
            }).end('bar');
        });
    });

    with_mqs(1, 'should be able to call done twice', function (mqs, cb)
    {
        var count = 0;

        mqs[0].client.subscribe('foo', function (s, info, done)
        {
            mqs[0].server.mux.on('handshake', function (duplex, hdata, delay)
            {
                expect(delay).to.equal(null);
                expect(hdata).to.eql(new Buffer([0]));
                
                count += 1;
                if (count === 1)
                {
                    setTimeout(function ()
                    {
                        expect(count).to.equal(1);
                        cb();
                    }, 3000);
                }
            });

            done();
            done(new Error('dummy'));
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.publish('foo', { single: true }, function (err)
            {
                if (err) { return cb(err);  }
            }).end('bar');
        });
    });

    with_mqs(1, 'should do nothing when done called for multi message',
    function (mqs, cb)
    {
        var count = 0;

        mqs[0].client.subscribe('foo', function (s, info, done)
        {
            mqs[0].server.mux.on('handshake', function (duplex, hdata, delay)
            {
                expect(delay).to.equal(null);
                expect(hdata).to.eql(new Buffer(0));
                
                count += 1;
                if (count === 1)
                {
                    setTimeout(function ()
                    {
                        expect(count).to.equal(1);
                        cb();
                    }, 3000);
                }
            });

            done();
            done(new Error('dummy'));
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.publish('foo', function (err)
            {
                if (err) { return cb(err);  }
            }).end('bar');
        });
    });

    with_mqs(1, 'server should error when work handshake is empty',
    function (mqs, cb)
    {
        mqs[0].server.fsq.once('warning', function (err)
        {
            expect(err.message).to.equal('buffer too small');
            cb();
        });

        mqs[0].client.subscribe('foo', function (s, info, done)
        {
            expect(info.single).to.equal(false);

            mqs[0].server.mux.once('handshake', function (duplex, hdata, delay)
            {
                expect(delay).to.equal(null);
                expect(hdata).to.eql(new Buffer(0));
            });

            done();
        }, function (err)
        {
            if (err) { return cb(err); }

            var listeners = mqs[0].client.mux.listeners('handshake');
            listeners.unshift(function (duplex, hdata, delay)
            {
                if (hdata.length > 1)
                {
                    hdata.writeUInt8(hdata.readUInt8(0) & ~1);
                }
            });

            mqs[0].client.mux.removeAllListeners('handshake');

            for (var l of listeners)
            {
                mqs[0].client.mux.on('handshake', l);
            }

            mqs[0].client.publish('foo', { single: true }, function (err)
            {
                if (err) { return cb(err);  }
            }).end('bar');
        });
    });

    with_mqs(1, 'should emit ack event', function (mqs, cb)
    {
        mqs[0].client.subscribe('foo', function (s, info, done)
        {
            mqs[0].server.on('ack', function (info)
            {
                expect(info.single).to.equal(true);
                expect(info.topic).to.equal('foo');
                cb();
            });

            done();
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.publish('foo', { single: true }, function (err)
            {
                if (err) { return cb(err);  }
            }).end('bar');
        });
    });

    with_mqs(1, 'should not publish message if stream errors', function (mqs, cb)
    {
        mqs[0].server.on('publish_requested', function (topic, stream, options, cb)
        {
            stream.pipe(this.fsq.publish(topic, options, cb));
            stream.emit('error', new Error('dummy'));
        });

        mqs[0].client.subscribe('foo', function ()
        {
            cb(new Error('should not be called'));
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.publish('foo', function (err)
            {
                expect(err.message).to.equal('server error');
                setTimeout(cb, 2000);
            }).end('bar');
        });
    });

    with_mqs(1, 'should not publish message if stream errors (>1 publish)', function (mqs, cb)
    {
        mqs[0].server.on('publish_requested', function (topic, stream, options, cb)
        {
            stream.pipe(this.fsq.publish(topic, options, cb));
            stream.pipe(this.fsq.publish(topic, options, cb));
            stream.emit('error', new Error('dummy'));
        });

        mqs[0].client.subscribe('foo', function ()
        {
            cb(new Error('should not be called'));
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.publish('foo', function (err)
            {
                expect(err.message).to.equal('server error');
                setTimeout(cb, 2000);
            }).end('bar');
        });
    });

    with_mqs(1, 'should not publish message if stream errors (0 publish)', function (mqs, cb)
    {
        mqs[0].server.on('publish_requested', function (topic, stream, options, cb)
        {
            stream.emit('error', new Error('dummy'));
            cb(new Error('dummy'));
        });

        mqs[0].client.subscribe('foo', function ()
        {
            cb(new Error('should not be called'));
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.publish('foo', function (err)
            {
                expect(err.message).to.equal('server error');
                setTimeout(cb, 2000);
            }).end('bar');
        });
    });

    with_mqs(1, 'should emit pre_subscribe_requested and pre_publish_requested events', function (mqs, cb)
    {
        var presubreq_called = false,
            prepubreq_called = false,
            subreq_called = false,
            pubreq_called = false;

        mqs[0].server.on('pre_subscribe_requested', function (topic, done)
        {
            presubreq_called = true;
            expect(topic).to.equal('foo');
            this.subscribe(topic, done);
        });

        mqs[0].server.on('subscribe_requested', function ()
        {
            subreq_called = true;
        });

        mqs[0].server.on('pre_publish_requested', function (topic, duplex, options, done)
        {
            prepubreq_called = true;
            expect(topic).to.equal('foo');
            duplex.pipe(this.fsq.publish(topic, options, done));
        });

        mqs[0].server.on('publish_requested', function ()
        {
            pubreq_called = true;
        });

        mqs[0].client.subscribe('foo', function (s, info)
        {
            expect(presubreq_called).to.equal(true);
            expect(prepubreq_called).to.equal(true);
            expect(subreq_called).to.equal(false);
            expect(pubreq_called).to.equal(false);
            expect(info.topic).to.equal('foo');
            read_all(s, function (v)
            {
                expect(v.toString()).to.equal('bar');
                cb();
            });
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.publish('foo', function (err)
            {
                if (err) { return cb(err); }
            }).end('bar');
        });
    });

    with_mqs(1, 'should emit error when end before handshake on message', function (mqs, cb)
    {
        mqs[0].server.fsq.once('warning', function (err)
        {
            expect(err.message).to.equal('ended before handshaken');
            cb();
        });

        mqs[0].server.on('message', function (data, info, multiplex, done)
        {
            multiplex().push(null);
        });

        mqs[0].client.subscribe('foo', function () {}, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.publish('foo', { single: true }, function (err)
            {
                if (err) { return cb(err); }
            }).end('bar');

        });
    });

    with_mqs(1, 'should emit error when end before handshake on server', function (mqs, cb)
    {
        function check()
        {
            var sle = mqs[0].server.last_error,
                cle = mqs[0].client.last_error;

            if (sle && cle)
            {
                expect(sle.message).to.equal('ended before handshaken');
                expect(cle.message).to.equal('ended before handshaken');
                cb();
                return true;
            }

            return false;
        }

        if (!check())
        {
            mqs[0].server.on('error', check);
            mqs[0].client.on('error', check);
        }
    },
    it,
    {
        skip_client_handshake: true,
        onmade: function (info)
        {
            info.server.on('error', function (err)
            {
                this.last_error = err;
            });

            info.client.on('error', function (err)
            {
                this.last_error = err;
            });

            info.server_stream.on('end', function ()
            {
                this.end();
            });

            info.client_stream.end();
        }
    });

    with_mqs(1, 'client should send back error for single message if no handlers are registered', function (mqs, cb)
    {
        var client_warning;

        mqs[0].client.on('warning', function (err)
        {
            client_warning = err.message;
        });

        mqs[0].server.on('warning', function (err)
        {
            expect(client_warning).to.equal('no handlers');
            expect(err.message).to.equal('client error');
            cb();
        });

        mqs[0].server.on('ack', function ()
        {
            cb(new Error('should not be called'));
        });

        mqs[0].server.subscribe('foo', function (err)
        {
            if (err) { return cb(err); }
            mqs[0].server.fsq.publish('foo',
            {
                single: true,
                ttl: 2000
            }).end();
        });
    });

    it('should emit error if carrier stream ends immediately', function (cb)
    {
        connect_and_accept(function (cs, ss)
        {
            var ended = false,
                errored = false;

            function check()
            {
                if (ended && errored)
                {
                    cb();
                }
            }

            cs.on('end', function ()
            {
                expect(ended).to.equal(false);
                ended = true;
                check();
            });

            ss.on('end', function ()
            {
                this.end();
            });

            cs.on('readable', function ()
            {
                this.read();
            });

            ss.on('readable', function ()
            {
                this.read();
            });

            cs.end();

            var mqclient = new MQlobberClient(cs);

            mqclient.on('error', function (err)
            {
                expect(errored).to.equal(false);
                expect(err.message).to.equal(
                    ended ? 'ended before handshaken' : 'write after end');
                errored = true;
                check();
            });
        });
    });

    with_mqs(1, 'should be able to supply extra data argument to done', function (mqs, cb)
    {
        var buf1 = crypto.randomBytes(64),
            buf2 = crypto.randomBytes(128),
            buf3 = crypto.randomBytes(256);

        mqs[0].server.on('subscribe_requested', function (topic, done)
        {
            expect(topic).to.equals('foo');
            this.subscribe(topic,
            {
                subscribe_to_existing: true
            }, function (err)
            {
                done(err, err ? undefined : buf1);
            });
        });

        mqs[0].server.on('unsubscribe_requested', function (topic, done)
        {
            expect(topic).to.equals('foo');
            this.unsubscribe(topic, function (err)
            {
                done(err, err ? undefined : buf2);
            });
        });

        mqs[0].server.on('publish_requested', function (topic, duplex, options, done)
        {
            expect(topic).to.equals('foo');
            duplex.pipe(this.fsq.publish(topic, options, function (err)
            {
                done(err, err ? undefined : buf3);
            }));
        });

        mqs[0].client.subscribe('foo', function (s, info)
        {
            expect(info.topic).to.equal('foo');
            read_all(s, function (v)
            {
                expect(v.toString()).to.equal('bar');
                mqs[0].client.unsubscribe('foo', undefined, function (err, data)
                {
                    if (err) { return cb(err); }
                    expect(data.equals(buf2)).to.equal(true);
                    cb();
                });
            });
        }, function (err, data)
        {
            if (err) { return cb(err); }
            expect(data.equals(buf1)).to.equal(true);
            mqs[0].client.publish('foo', function (err, data)
            {
                if (err) { return cb(err); }
                expect(data.equals(buf3)).to.equal(true);
            }).end('bar');
        });
    });

    with_mqs(1, 'should be able to subscribe to existing messages', function (mqs, cb)
    {
        var handler1_called = 0,
            handler2_called = 0,
            handler3_called = 0;

        function check()
        {
            // Because neither server or client have separate handlers for each
            // subscription, all subscribers (which match the topic) get
            // existing messages when a new subscription to existing messages
            // is made.

            if ((handler1_called === 3) &&
                (handler2_called === 2) &&
                (handler3_called === 1))
            {
                return cb();
            }

            if ((handler1_called > 3) ||
                (handler2_called > 2) ||
                (handler3_called > 1))
            {
                return cb(new Error('called too many times'));
            }
        }

        mqs[0].client.subscribe('foo', function (s, info)
        {
            expect(info.topic).to.equal('foo');

            handler1_called += 1;
            check();
            if (handler1_called > 1)
            {
                return;
            }

            read_all(s, function (v)
            {
                expect(v.toString()).to.equal('bar');

                mqs[0].server.on('subscribe_requested', function (topic, done)
                {
                    expect(topic).to.be.oneOf(['foo.#', '#.foo']);
                    this.subscribe(topic,
                    {
                        subscribe_to_existing: true
                    }, function (err)
                    {
                        done(err, err ? undefined : new Buffer([1]));
                    });
                });
                
                mqs[0].client.subscribe('foo.#', function (s, info)
                {
                    expect(info.topic).to.equal('foo');
                    expect(info.existing).to.equal(true);

                    var pthru = new stream.PassThrough();
                    s.pipe(pthru);
                    read_all(pthru, function (v)
                    {
                        expect(v.toString()).to.equal('bar');

                        handler2_called += 1;
                        check();
                        if (handler2_called > 1)
                        {
                            return;
                        }

                        mqs[0].client.subscribe('#.foo', function (s, info)
                        {
                            expect(info.topic).to.equal('foo');
                            expect(info.existing).to.equal(true);

                            var pthru = new stream.PassThrough();
                            s.pipe(pthru);
                            read_all(pthru, function (v)
                            {
                                expect(v.toString()).to.equal('bar');
                                handler3_called += 1;
                                check();
                            });
                        }, function (err, data)
                        {
                            if (err) { return cb(err); }
                            expect(data.length).to.equal(1);
                            expect(data[0]).to.equal(1);
                        });
                    });
                }, function (err, data)
                {
                    if (err) { return cb(err); }
                    expect(data.length).to.equal(1);
                    expect(data[0]).to.equal(1);
                });
            });
        }, function (err, data)
        {
            if (err) { return cb(err); }
            expect(data).to.equal(undefined);
            mqs[0].client.publish('foo', function (err)
            {
                if (err) { return cb(err); }
            }).end('bar');
        });
    });
});
};
