var net = require('net'),
    QlobberFSQ = require('qlobber-fsq').QlobberFSQ,
    mqlobber = require('../..'),
    MQlobberServer = mqlobber.MQlobberServer,
    fsq = new QlobberFSQ();

fsq.on('start', function ()
{
    var server = net.createServer().listen(parseInt(process.argv[2]));
    server.on('connection', function (c)
    {
        new MQlobberServer(fsq, c);
    });
//--------------------
    server.on('connection', function (c)
    {
        c.on('end', function ()
        {
            if (process.send)
            {
                process.send('disconnect');
            }
        });

        if (process.send)
        {
            process.send('connect');
        }
    });

    server.on('listening', function ()
    {
        if (process.send)
        {
            process.send('listening');
        }
    });

    process.on('message', function ()
    {
        process.removeAllListeners('message');
        fsq.stop_watching();
        server.close();
    });
//--------------------
});

