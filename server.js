/*
Just supply qlobber-fsq obj and stream. Multiplex (using bpmux) off that.
No user/auth stuff here.

Messages (published and received) on separate streams. Use handshake data
to describe the messages.

Subscribe, unsubscribe on a control channel. Use frame-stream

Also allow subscribe, unsubscribe from not in the stream.

So take:

- fsq object; up to caller to specify dedup - without, client may receive
  messages multiple times
- stream
- options:
    - handshake data for the control channel (e.g. up to caller if knows single
      is disabled to send that info if the client can deal with it)
    - whether to use fastest-writable
    - options for bpmux, frame-stream, fastest-writable

Exposes:

- subscribe (doesn't take handler)
- unsubscribe (doesn't take handler) - no topic means unsubscribe all
- publish
- events:
  - subscribe_requested (if no handler then calls subscribe)
  - unsubscribe_requested (if no handler then calls unsubscribe)
  - publish_requested (if no handler then calls publish)
  - handshake (on control channel only)

Lifecycle:

- Don't replicate lifecycle events of fsq obj or stream
- Only care about cleaning up our own state. 
  - On stream end or finish, unsubscribe all

Misc:

- Need to remember all active subscriptions so can remove when unsubscribe all
  Might as well use this to stop multiple subscriptions for same subject
  (although fsq dedup also takes care of that)

- How use fastest-writable? We'll have multiple mqlobber objects each with
  their own handler, so the data will be written at the speed of the slowest.
  We should put a fastest-writable onto the stream as a property, and pipe the
  stream onto it (first time we create the fw). Add our stream as peer on fw.
  (Alternative is to use info to store fw).
    - Make this optional

- We want to use the filter handler function to prevent the message being
  delivered if all streams are full.
    - How detect stream is full? write empty data
    - But the filter handler is passed when the fsq is constructed.
      This is optional behaviour, expose a filter function which caller
      can use when constructing fsq
*/

var EventEmitter = require('events').EventEmitter,
    BPMux = require('bpmux').BPMux,
    frame = require('frame-stream'),
    FastestWritable = require('fastest-writable').FastestWritable,
    util = require('util'),
    TYPE_SUBSCRIBE = 0,
    TYPE_UNSUBSCRIBE = 1;

function MQlobber(fsq, stream, options)
{
    EventEmitter.call(this);

    options = options || {};

    this._fsq = fsq;
    this._mux = new BPMux(stream, options);
    this._subs = new Set();
    this._fastest_writable = options.fastest_writable;
    this._done = false;

    var ths = this;

    this._mux.on('error', function (err)
    {
        ths.emit('error', err);
    });

    function done()
    {
        ths.unsubscribe();
        ths._done = true;
    }

    stream.on('end', done);
    stream.on('finish', done);

    this._handler = function (data, info, cb)
    {
        var hdata = new Buffer(
        single
        expires
        topic

        ths._mux.multiplex({ handshake_data: hdata }, function (err, duplex)
        {
            if (err)
            {
                return ths.emit('error', err);
            }

            data.on('end', cb);
            data.pipe(duplex);
        });
    };

    this._handler.accept_stream = true;

    this._mux.once('handshake', function (duplex, hdata, delay)
    {
        ths._control = frame.decode(options);
        duplux.pipe(this._control);

        ths._control.on('readable', function ()
        {
            var data = this.read();

            if (data.length === 0)
            {
                return ths.emit('error', new Error('empty buffer'));
            }

            var type = buf.readUInt8(0, true);

            switch (type)
            {
                case TYPE_SUBSCRIBE:
                    var topic = buf.toString('utf8', 1);
                    if (!ths.emit('subscribe_requested', topic))
                    {
                        ths.subscribe(topic);
                    }
                    break;

                case TYPE_UNSUBSCRIBE:
                    var topic = buf.toString('utf8', 1);
                    if (!ths.emit('unsubscribe_requested', topic))
                    {
                        ths.unsubscribe(topic);
                    }
                    break;

                default:
                    ths.emit('error', new Error('unknown type:' + type)
                    break;
            }
        });

        this.on('handshake', function (duplex, hdata)
        {
            if (hdata.length === 0)
            {
                return ths.emit('error', new Error('empty buffer'));
            }

            var options = { single: hdata.readUInt8(0, true) },
                topic = hdata.toString('utf8', 1);

            if (!ths.emit('publish_requested', topic, duplex, options))
            {
                duplex.pipe(ths.publish(topic, options));
            }
        });

        ths.emit('handshake', hdata, delay);
    });
}

util.inherits(MQlobber, EventEmitter);

MQlobber.prototype.subscribe = function (topic)
{
    if (!this._done && !this._subs.has(topic))
    {
        this._fsq.subscribe(topic, this._handler);
        this._subs.add(topic);
    }
};

MQlobber.prototype.unsubscribe = function (topic)
{
    if (topic === undefined)
    {
        for (var topic of this._subs)
        {
            this._fsq.unsubscribe(topic, this._handler);
        }
        this._subs.clear();
    }
    else if (this._subs.has(topic))
    {
        this._fsq.unsubscribe(topic, this._handler);
    }
};

MQlobber.prototype.publish = function (topic, payload, options)
{
    return this._fsq.publish(topic, payload, options);
};

// have a single handler for messages
// re-emit error on fastest-writable
// filter function
