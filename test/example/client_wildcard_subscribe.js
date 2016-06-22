var assert = require('assert'),
    MQlobberClient = require('mqlobber').MQlobberClient,
    c = require('net').createConnection(parseInt(process.argv[2])),
    mq = new MQlobberClient(c);

mq.subscribe('foo.*', function (s, info)
{
    assert.equal(info.topic, 'foo.bar');
    var msg = '';
    s.on('readable', function ()
    {
        var data;
        while ((data = this.read()) !== null)
        {
            msg += data.toString();
        }
    });
    s.on('end', function ()
    {
        console.log('wildcard received', info.topic, msg);
        assert.equal(msg, 'hello');
        c.end();
    });
}
//--------------------
, function () { process.send('subscribed'); }
//--------------------
);
