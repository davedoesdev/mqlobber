var cp = require('child_process'),
    path = require('path'),
    async = require('async'),
    count = 0,
    servers = [],
    base_port = 8600;

async.times(2, function (n, next)
{
    cp.fork(path.join(__dirname, 'server.js'), [base_port + n]).on('message', function (m)
    {
        switch (m)
        {
            case 'listening':
                next(null, this);
                break;

            case 'connect':
                count += 1;
                break;

            case 'disconnect':
                count -= 1;
                if (count === 0)
                {
                    for (var server of servers)
                    {
                        server.send('stop');
                    }
                }
                break;
        }
    });
}, function (err, svrs)
{
    servers = svrs;

    cp.fork(path.join(__dirname, 'client_subscribe.js'), [base_port]).on('message', function ()
    {
        cp.fork(path.join(__dirname, 'client_wildcard_subscribe.js'), [8601]).on('message', function ()
        {
            cp.fork(path.join(__dirname, 'client_publish.js'), [base_port]);
        })
    });
});
