/*jshint mocha: true */
"use strict";

var stream = require('stream'),
    util = require('util'),
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
    rabbitmq_bindings = require('./rabbitmq_bindings');

function read_all(s, cb)
{
    var bufs = [];

    s.on('end', function ()
    {
        cb(Buffer.concat(bufs));
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

function topic_sort(a, b)
{
    return parseInt(a.substr(1), 10) - parseInt(b.substr(1), 10);
}

var timeout = 10 * 60;

module.exports = function (description, connect_and_accept)
{
    function with_mqs(n, description, f, mqit, options)
    {
        describe('mqs=' + n, function ()
        {
            var fsq, mqs, ended = false;

            before(function (cb)
            {
                var fsq_dir = path.join(path.dirname(require.resolve('qlobber-fsq')), 'fsq');
                rimraf(fsq_dir, cb);
            });

            before(function (cb)
            {
                this.timeout(timeout * 1000);

                fsq = new QlobberFSQ(
                {
                    multi_ttl: timeout * 1000,
                    single_ttl: timeout * 2 * 1000
                });

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
                                      }, options));

                            cmq.on('handshake', function ()
                            {
                                cb(null, {
                                    client: cmq,
                                    server: smq,
                                    client_stream: cs,
                                    server_stream: ss
                                });
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
/*
    with_mqs(1, 'should publish and receive a message on single stream',
    function (mqs, cb)
    {
        mqs[0].client.subscribe('foo', function (s, info)
        {
            expect(info.single).to.equal(false);
            expect(info.topic).to.equal('foo');

            var now = Date.now(), expires = info.expires * 1000;

            expect(expires).to.be.above(now);
            expect(expires).to.be.below(now + timeout * 1000);

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
            this.timeout(timeout * 1000);

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
        this.timeout(5000);
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
                mqs[0].client.unsubscribe('foo', handler2, function (err)
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

    with_mqs(1, 'should unsubscribe topic', function (mqs, cb)
    {
        this.timeout(5000);
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
        this.timeout(5000);
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

        mqs[0].server._mux.multiplex();
    });

    with_mqs(1, 'client should warn about short handshake data', function (mqs, cb)
    {
        mqs[0].client.on('warning', function (err, duplex)
        {
            expect(err.message).to.equal('buffer too small');
            expect(duplex).to.be.an.instanceof(stream.Duplex);
            cb();
        });

        mqs[0].server._mux.multiplex(
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

        mqs[0].server._mux.on('handshake', function (duplex, hdata, delay)
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

        mqs[0].server._mux.on('handshake', function (duplex, hdata, delay)
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
        var got_warning = false;

        mqs[0].server.on('publish_requested', function (topic, duplex, options, done)
        {
            done(new Error('test error'));
        });

        mqs[0].server.on('warning', function (err, duplex)
        {
            expect(err.message).to.equal('test error');
            expect(duplex).to.be.an.instanceof(stream.Duplex);
            got_warning = true;
        });

        mqs[0].client.publish('foo', function (err)
        {
            expect(err.message).to.equal('server error');
            expect(got_warning).to.equal(true);
            cb();
        }).end('bar');
    });

    with_mqs(1, 'should warn about short return handshakes from server when publishing',
    function (mqs, cb)
    {
        mqs[0].server.on('publish_requested', function (topic, done)
        {
            // stop server handshake handler replying
        });

        mqs[0].server._mux.on('handshake', function (duplex, hdata, delay)
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
        mqs[0].client._mux.emit('error', new Error('test error'));
    });

    with_mqs(1, 'server should emit error event when mux errors',
    function (mqs, cb)
    {
        mqs[0].server.on('error', function (err)
        {
            expect(err.message).to.equal('test error');
            cb();
        });
        mqs[0].server._mux.emit('error', new Error('test error'));
    });

    with_mqs(1, 'client should throw exception when called after stream finishes', function (mqs, cb, end)
    {
        end(function ()
        {
            expect(function ()
            {
                mqs[0].client.subscribe('foo', function ()
                {
                    cb(new Error('should not be called'));
                });
            }).to.throw('finished');
            
            expect(function ()
            {
                mqs[0].client.unsubscribe();
            }).to.throw('finished');

            expect(function ()
            {
                mqs[0].client.publish('foo').end('bar');
            }).to.throw('finished');

            cb();
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
            duplex.pipe(this._fsq.publish(topic, options, function (err)
            {
                if (err) { return cb(err); }
                done();

                mqs[0].server.once('subscribe_requested', function (topic, done)
                {
                    expect(topic).to.equal('foo');
                    this.subscribe(topic, function (err)
                    {
                        if (err) { return cb(err); }
                        done();
                        mqs[0].client.unsubscribe();
                    });
                });

                mqs[0].client.subscribe('foo', function (s)
                {
                    cb(new Error('should not be called'));
                });
            }));
        });

        mqs[0].server.on('unsubscribe_requested', function (topic, done)
        {
            expect(topic).to.equal('foo');
            this.unsubscribe(topic, function (err)
            {
                if (err) { return cb(err); }
                done();
                mqs[0].client.publish('foo').end('bar');
            });
        });

        mqs[0].server.once('subscribe_requested', function (topic, done)
        {
            expect(topic).to.equal('foo');
            this.subscribe(topic, function (err)
            {
                if (err) { return cb(err); }
                done();
                mqs[0].client.unsubscribe('foo');
            });
        });

        mqs[0].client.subscribe('foo', function (s)
        {
            cb(new Error('should not be called'));
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

    with_mqs(1, 'should emit full event when client handshakes are backed up', function (mqs, cb)
    {
        this.timeout(2 * 60 * 1000);

        var orig_write = mqs[0].client_stream._write,
            the_chunk,
            the_encoding,
            the_callback,
            count_complete = 0,
            count_incomplete = 0,
            full_called = false;

        mqs[0].client_stream._write = function (chunk, encoding, callback)
        {
            the_chunk = chunk;
            the_encoding = encoding;
            the_callback = callback;
        };

        // number will change if bpmux handhsake buffer size changes

        mqs[0].client.on('full', function ()
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
                mqs[0].client_stream._write = orig_write;
                mqs[0].client_stream._write(the_chunk, the_encoding, the_callback);
                cb();
            }
        }

        for (var i=0; i < 2993; i += 1)
        {
            var duplex = mqs[0].client.publish('foo');
            duplex.on('handshake_sent', sent);
            duplex.end('bar');
        }
    });

    with_mqs(1, 'should publish and receive work on single stream',
    function (mqs, cb)
    {
        mqs[0].client.subscribe('foo', function (s, info)
        {
            expect(info.single).to.equal(true);
            expect(info.topic).to.equal('foo');

            var now = Date.now(), expires = info.expires * 1000;

            expect(expires).to.be.above(now);
            expect(expires).to.be.below(now + timeout * 2 * 1000);

            read_all(s, function (v)
            {
                expect(v.toString()).to.equal('bar');
                cb();
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
        this.timeout(5000);

        var calls = 0, data = '';

        function check(s, info)
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
            });

            calls += 1;
        }

        mqs[0].client.subscribe('foo', function (s, info)
        {
            check(s, info);
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.subscribe('foo', function (s, info)
            {
                check(s, info);
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
        this.timeout(5000);

        var calls = 0, data = '';

        function check(s, info)
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
            });

            calls += 1;
        }

        mqs[0].client.subscribe('foo', function (s, info)
        {
            check(s, info);
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[1].client.subscribe('foo', function (s, info)
            {
                check(s, info);
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

        mqs[0].server._mux.multiplex();
    }, it, { sinon: true });

    with_mqs(1, 'server should warn about empty handshake data', function (mqs, cb)
    {
        mqs[0].server.on('warning', function (err, duplex)
        {
            expect(err.message).to.equal('buffer too small');
            expect(duplex).to.be.an.instanceof(stream.Duplex);
            cb();
        });

        mqs[0].client._mux.multiplex();
    });

    with_mqs(1, 'server should warn about short handshake data (no flags)', function (mqs, cb)
    {
        mqs[0].server.on('warning', function (err, duplex)
        {
            expect(err.message).to.equal('buffer too small');
            expect(duplex).to.be.an.instanceof(stream.Duplex);
            cb();
        });

        mqs[0].client._mux.multiplex(
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

        mqs[0].client._mux.multiplex(
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

        mqs[0].client._mux.multiplex(
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

        mqs[0].client._mux.multiplex();
    }, it, { sinon: true });

    with_mqs(1, 'should emit full event when server handshakes are backed up', function (mqs, cb)
    {
        this.timeout(2 * 60 * 1000);

        var orig_write = mqs[0].server_stream._write,
            the_chunk,
            the_encoding,
            the_callback,
            count_complete = 0,
            count_incomplete = 0,
            full_called = false;

        mqs[0].server_stream._write = function (chunk, encoding, callback)
        {
            the_chunk = chunk;
            the_encoding = encoding;
            the_callback = callback;
        };

        // number will change if bpmux handhsake buffer size changes

        mqs[0].server.on('full', function ()
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
            expect(info.expires).to.be.below(now + timeout * 1000);

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

    with_mqs(1, 'should publish and receive work with ttl',
    function (mqs, cb)
    {
        mqs[0].client.subscribe('foo', function (s, info)
        {
            expect(info.single).to.equal(true);
            expect(info.topic).to.equal('foo');

            var now = Date.now(), expires = info.expires * 1000;

            expect(expires).to.be.above(now);
            expect(expires).to.be.below(now + timeout * 1000);

            read_all(s, function (v)
            {
                expect(v.toString()).to.equal('bar');
                cb();
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
        mqs[0].server._fsq.subscribe = function (topic, handler, cb)
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

        mqs[0].server._fsq.subscribe = function (topic, handler, cb)
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
        var orig_unsubscribe = mqs[0].server._fsq.unsubscribe;

        mqs[0].server._fsq.unsubscribe = function (topic, handler, cb)
        {
            cb(new Error('test'));
        };

        mqs[0].server.subscribe('foo', function (err)
        {
            if (err) { return cb(err); }

            mqs[0].server.unsubscribe(function (err)
            {
                expect(err.message).to.equal('test');
                mqs[0].server._fsq.unsubscribe = orig_unsubscribe;
                cb();
            });
        });
    });
 
    with_mqs(1, 'server should pass on fsq errors in its unsubscribe(topic) method',
    function (mqs, cb)
    {
        var orig_unsubscribe = mqs[0].server._fsq.unsubscribe;

        mqs[0].server._fsq.unsubscribe = function (topic, handler, cb)
        {
            cb(new Error('test'));
        };

        mqs[0].server.subscribe('foo', function (err)
        {
            if (err) { return cb(err); }

            mqs[0].server.unsubscribe('foo', function (err)
            {
                expect(err.message).to.equal('test');
                mqs[0].server._fsq.unsubscribe = orig_unsubscribe;
                cb();
            });
        });
    });

    with_mqs(1, 'server should warn about fsq errors in its unsubscribe method',
    function (mqs, cb)
    {
        this.sinon.stub(console, 'error');

        var orig_unsubscribe = mqs[0].server._fsq.unsubscribe;

        mqs[0].server._fsq.unsubscribe = function (topic, handler, cb)
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
                mqs[0].server._fsq.unsubscribe = orig_unsubscribe;
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
*/
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
                var t2 = 't' + (Math.floor((parseInt(t.substr(1), 10) - 1) / topics_per_mq) + 1)

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
                this.timeout(5 * 1000);

                var results = {},
                    results_single = new Map(),
                    total = 0,
                    count = 0,
                    count_single = 0;

                for (var i = 0; i < expected.length; i += 1)
                {
                    total += expected[i][1].length * rounds;
                }

                if (total === 0)
                {
                    setTimeout(function ()
                    {
                        expect(count).to.equal(0);
                        cb();
                    }, 3000);
                }

                async.times(rabbitmq_bindings.test_bindings.length, function (i, cb2)
                {
                    var n = Math.floor(i / topics_per_mq);

                    function handler(s, info)
                    {
                        s.setMaxListeners(0);

                        var pthru = new stream.PassThrough();
                        s.pipe(pthru);

                        read_all(pthru, function (v)
                        {
                            expect(v.toString()).to.equal(info.topic);

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

                                cb();
                            }
                            else if (count > total)
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

    // start with single server, do multi-process server later (clustered?)
};

/*
test fastest-writable:

mq_server.on('message', function (msg_stream, dest, info)
{
    if (!msg_stream.fastest_writable)
    {
        msg_stream.fastest_writable = new FastestWritable();
        msg_stream.pipe(msg_stream.fastest_writable);
    }

    msg_stream.add_peer(dest);
});

test filter_all_drained:

function filter_all_drained(info, handlers, cb)
{
    for (var h of handlers)
    {
        if (h.mqlobber_stream &&
            (h.mqlobber_stream._writableState.length >=
             h.mqlobber_stream._writableState.highWaterMark))
        {
            return cb(null, false);
        }
    }

    cb(null, true, handlers);
}
*/
