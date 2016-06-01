/*jshint mocha: true */
"use strict";

var stream = require('stream'),
    async = require('async'),
    mqlobber = require('..'),
    MQlobberClient = mqlobber.MQlobberClient,
    MQlobberServer = mqlobber.MQlobberServer,
    QlobberFSQ = require('qlobber-fsq').QlobberFSQ,
    chai = require('chai'),
    expect = chai.expect;

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

var timeout = 10 * 60;

module.exports = function (description, connect, accept)
{
    function with_mqs(n, description, f, mqit)
    {
        describe('mqs=' + n, function ()
        {
            var fsq, mqs;

            before(function (cb)
            {
                this.timeout(timeout * 1000);

                fsq = new QlobberFSQ(
                {
                    multi_ttl: timeout * 1000,
                    single_ttl: timeout * 1000
                });

                fsq.on('start', function ()
                {
                    async.times(n, function (i, cb)
                    {
                        connect(function (cs)
                        {
                            accept(cs, function (ss)
                            {
                                var cmq = new MQlobberClient(cs),
                                    smq = new MQlobberServer(fsq, ss, { send_expires: true });

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
                        });
                    }, function (err, v)
                    {
                        mqs = v;
                        cb(err);
                    });
                });
            });

            after(function (cb)
            {
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
            });

            (mqit || it)(description, function (cb)
            {
                f.call(this, mqs, cb);
            });
        });
    }

    with_mqs(1, 'should publish and receive a message on single stream',
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
                    mqs[0].client.unsubscribe('foo', function (err)
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

    with_mqs(1, 'should warn about empty handshake data', function (mqs, cb)
    {
        mqs[0].client.on('warning', function (err, obj)
        {
            expect(err.message).to.equal('buffer too small');
            expect(obj).to.be.an.instanceof(stream.Duplex);
            cb();
        });

        mqs[0].server._mux.multiplex();
    });

    with_mqs(1, 'should warn about short handshake data', function (mqs, cb)
    {
        mqs[0].client.on('warning', function (err, obj)
        {
            expect(err.message).to.equal('buffer too small');
            expect(obj).to.be.an.instanceof(stream.Duplex);
            cb();
        });

        mqs[0].server._mux.multiplex(
        {
            handshake_data: new Buffer([2])
        });
    });
    
    // errors
    // need to do single messages - will need to remove pre-existing ones
    // full events and drain on carrier
    // support without sending expiry
    // try to get 100% coverage
    // rabbitmq etc - see qlobber-fsq tests
    // tcp streams
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
