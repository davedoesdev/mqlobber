var assert = require('assert'),
    mqlobber = require('../..'),
    MQlobberClient = mqlobber.MQlobberClient,
    c = require('net').createConnection(parseInt(process.argv[2])),
    mq = new MQlobberClient(c);

mq.publish(process.argv[3], function (err)
{
    assert.ifError(err);
    c.end();
}).end('hello');
