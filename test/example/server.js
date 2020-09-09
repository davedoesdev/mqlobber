var net = require('net'),
    QlobberFSQ = require('qlobber-fsq').QlobberFSQ,
    mqlobber = require('../..'),
    MQlobberServer = mqlobber.MQlobberServer,
    config = require('config'),
    fsq;
    
if (process.env.USE_QLOBBER_PG)
{
    var QlobberPG = require('qlobber-pg').QlobberPG;
    fsq = new QlobberPG(Object.assign(
    {
        name: 'test'
    }, config));
}
else
{
    fsq = new QlobberFSQ();
}

fsq.on('start', function ()
{
    var server = net.createServer().listen(parseInt(process.argv[2]));
    server.on('connection', function (c)
    {
        new MQlobberServer(fsq, c).on('error', console.error);
    });
//--------------------
    server.on('connection', function (c)
    {
        var disconnected = false;

        function disconnect()
        {
            if (process.send && !disconnected)
            {
                disconnected = true;
                process.send('disconnect');
            }
        }

        c.on('end', disconnect);
        c.on('close', disconnect);

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

