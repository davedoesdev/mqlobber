"use strict";

var net = require('net'),
    common = require('./runner');

var server = net.createServer();
server.listen(8600);

// assume runner calls this in series
function connect_and_accept(cb)
{
    var cs;

    server.once('connection', function (ss)
    {
        cb(cs, ss);
    });

    cs = net.createConnection(8600);
}

common('tcp', connect_and_accept);
