var cp = require('child_process'),
    path = require('path'),
    async = require('async');

describe('example', function ()
{
    it('subscribers should connect, receive then disconnect', function (done)
    {
        var count = 0,
            servers = [],
            base_port = 8700;

        async.times(2, function (n, next)
        {
            cp.fork(path.join(__dirname, 'server.js'), [base_port + n]).on('message', function (m)
            {
                var exits = 0;
                function exit()
                {
                    exits += 1;
                    if (exits === servers.length)
                    {
                        done();
                    }
                }

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
                                server.on('exit', exit);
                                server.send('stop');
                            }
                        }
                        break;
                }
            });
        }, function (err, svrs)
        {
            servers = svrs;

            cp.fork(path.join(__dirname, 'client_subscribe.js'), [base_port, 'foo.bar']).on('message', function ()
            {
                cp.fork(path.join(__dirname, 'client_subscribe.js'), [base_port + 1, 'foo.*']).on('message', function ()
                {
                    cp.fork(path.join(__dirname, 'client_publish.js'), [base_port, 'foo.bar']);
                });
            });
        });
    });
});
