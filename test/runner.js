/*jshint mocha: true */
"use strict";

var async = require('async'),
    mqlobber = require('..'),
    MQlobberClient = mqlobber.MQlobberClient,
    MQlobberServer = mqlobber.MQlobberServer,
    QlobberFSQ = require('qlobber-fsq').QlobberFSQ;

// start with single stream, do multiple streams later
//   but perhaps we should have concept of separate streams now
//   (e.g. for pub and sub) even if we use same stream
//   maybe test should specify how many streams to make
// start with single server, do multi-process server later (clustered?)

module.exports = function (description, connect, accept)
{
    function with_mqs(n, description, f)
    {
        describe('mqs=' + n, function ()
        {
            var fsq, mqs;

            before(function (cb)
            {
                fsq = new QlobberFSQ();
                fsq.on('start', function ()
                {
                    async.times(n, function (i, cb)
                    {
                        connect(function (cs)
                        {
                            accept(cs, function (ss)
                            {
                                var cmq = new MQlobberClient(cs),
                                    smq = new MQlobberServer(fsq, ss);

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

            it(description, function (cb)
            {
                f(mqs, cb);
            });
        });
    }

    with_mqs(1, 'should publish and receive a message on single stream',
    function (mqs, cb)
    {
        mqs[0].client.subscribe('foo', function (s, info)
        {
            cb();
        }, function (err)
        {
            if (err) { return cb(err); }
            mqs[0].client.publish('foo', function (err, s)
            {
                if (err) { return cb(err); }
                s.end('bar');
            });
        });
    });
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
