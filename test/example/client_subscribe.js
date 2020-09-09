/*jshint laxcomma: true */
//--------------------
var assert = require('assert'),
    MQlobberClient = require('../..').MQlobberClient,
    c = require('net').createConnection(parseInt(process.argv[2])),
    mq = new MQlobberClient(c),
    topic = process.argv[3];

mq.subscribe(topic, function (s, info)
{
    var msg = '';
    s.on('readable', function ()
    {
        var data;
        while ((data = this.read()) !== null)
        {
            msg += data.toString();
        }
    });
    s.on('finish', function ()
    {
        c.end();
    });
    s.on('end', function ()
    {
        console.log('received', info.topic, msg);
        assert.equal(msg, 'hello');
    });
}
//--------------------
, function (err)
{
    assert.ifError(err);
    if (process.send)
    {
        process.send('subscribed');
    }
}
//--------------------
);
