var assert = require('assert'),
    cp = require('child_process'),
    path = require('path'),
    async = require('async');

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

function drain(p, cb)
{
    var stdout, stderr;

    read_all(p.stdout, function (v)
    {
        stdout = v;
    });
    read_all(p.stderr, function (v)
    {
        stderr = v;
    });

    p.on('close', function (code)
    {
        cb(code, stdout, stderr);
    });
}

describe('example', function ()
{
    this.timeout(60000);

    it('subscribers should connect, receive then disconnect', function (done)
    {
        var count = 0,
            servers = [],
            base_port = 8700;

        async.times(2, function (n, next)
        {
            var p = cp.fork(path.join(__dirname, 'server.js'), [base_port + n], { silent: true });

            drain(p, function (code, stdout, stderr)
            {
                var s, pos1, pos2, pos3;
                assert.equal(code, 0);
                assert.equal(stdout.length, 0);
                if (process.env.USE_QLOBBER_PG === '1')
                {
                    s = stderr.toString();
                    pos1 = s.indexOf('Error: stopped');
                    pos2 = s.indexOf('Error: Connection terminated');
                    pos3 = s.indexOf('Error: Client was closed and is not queryable');
                    assert((stderr.length === 0) || (pos1 === 0) || (pos2 === 0) || (pos3 === 0));
                }
                else
                {
                    assert.equal(stderr.length, 0);
                }
            });

            p.on('message', function (m)
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
                                server.on('close', exit);
                                server.send('stop');
                            }
                        }
                        break;
                }
            });
        }, function (err, svrs)
        {
            servers = svrs;

            var p = cp.fork(path.join(__dirname, 'client_subscribe.js'), [base_port, 'foo.bar'], { silent: true });

            drain(p, function (code, stdout, stderr)
            {
                assert.equal(code, 0);
                assert.equal(stdout.toString(), 'received foo.bar hello\n');
                assert.equal(stderr.length, 0);
            });

            p.on('message', function ()
            {
                p = cp.fork(path.join(__dirname, 'client_subscribe.js'), [base_port + 1, 'foo.*'], { silent: true});

                drain(p, function (code, stdout, stderr)
                {
                    assert.equal(code, 0);
                    assert.equal(stdout.toString(), 'received foo.bar hello\n');
                    assert.equal(stderr.length, 0);
                });

                p.on('message', function ()
                {
                    cp.fork(path.join(__dirname, 'client_publish.js'), [base_port, 'foo.bar']);
                });
            });
        });
    });
});
