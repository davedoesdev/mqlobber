/*jshint mocha: true */
"use strict";

var mqlobber = require('..'),
    MQlobberClient = mqlobber.MQlobberClient,
    MQlobberServer = mqlobber.MQlobberServer,
    QlobberFSQ = require('qlobber-fsq').QlobberFSQ;

// start with single stream, do multiple streams later
//   but perhaps we should have concept of separate streams now
//   (e.g. for pub and sub) even if we use same stream
//   maybe test should specify how many streams to make
// start with single server, do multi-process server later (clustered?)

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

module.exports = function (description, connect, accept)
{
    describe(description, function ()
    {
        var client_stream,
            server_stream,
            fsq,
            mq_client,
            mq_server;

        before(function (cb)
        {
            connect(function (cs)
            {
                client_stream = cs;
                accept(cs, function (ss)
                {
                    server_stream = ss;
                    cb();
                });
            });
        });

        after(function (cb)
        {
            client_stream.on('end', cb);
            client_stream.end();
            fsq.stop_watching(cb);
        });

        before(function (cb)
        {
            mq_client = new MQlobberClient(client_stream);
            mq_client.on('handshake', function ()
            {
                cb();
            });
            fsq = new QlobberFSQ();
            fsq.on('start', function ()
            {
                mq_server = new MQlobberServer(fsq, server_stream);
            });
        });

        it('should publish and receive a message', function (cb)
        {
            mq_client.subscribe('foo', function (s, info)
            {
                cb();
            }, function (err)
            {
                if (err) { return cb(err); }
                mq_client.publish('foo', function (err, s)
                {
                    if (err) { return cb(err); }
                    s.end('bar');
                });
            });
        });
    });
};
