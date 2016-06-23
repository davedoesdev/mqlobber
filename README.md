# mqlobber&nbsp;&nbsp;&nbsp;[![Build Status](https://travis-ci.org/davedoesdev/mqlobber.png)](https://travis-ci.org/davedoesdev/mqlobber) [![Coverage Status](https://coveralls.io/repos/davedoesdev/mqlobber/badge.png?branch=master&service=github)](https://coveralls.io/r/davedoesdev/mqlobber?branch=master) [![NPM version](https://badge.fury.io/js/mqlobber.png)](http://badge.fury.io/js/mqlobber)

Streaming message queue with pub-sub, work queues, wildcards and back-pressure.
Just Node and a filesystem required.

`mqlobber` basically remotes 
[`qlobber-fsq`](https://github.com/davedoesdev/qlobber-fsq) over one or more
connections.

<p align="center"><img src="diagrams/overview.svg" width="80%"/></p>

Say you have a server and a number of clients, with the clients
connected to the server using some mechanism which provides a stream for each
connection. Create a `QlobberFSQ` instance on the server and for each stream,
pass the instance and the stream to `MQlobberServer`.

On each client, pass the other end of the stream to `MQlobberClient`. Clients
can then publish and subscribe to topics (including wildcard subscriptions).
Work queues are also supported - when publishing a message, a client can specify
that only one subscriber should receive it.

All data is transferred on streams multiplexed over each connection using
[`bpmux`](https://github.com/davedoesdev/bpmux), with full back-pressure support
on each stream. Clients get a `Writable` when publishing a message and a
`Readable` when receiving one.

You can scale out horizontally by creating a number of `QlobberFSQ` instances
(e.g. one per CPU core), all sharing the same message directory.

<p align="center"><img src="diagrams/multi_instance.svg" width="80%"/></p>

No other backend services are required - just Node and a filesystem.

The API is described [here](#tableofcontents).

## Example

First, let's create a server program which listens on a TCP port specified on
the command line:

```javascript
// server.js
var net = require('net'),
    QlobberFSQ = require('qlobber-fsq').QlobberFSQ,
    MQlobberServer = require('mqlobber').MQlobberServer,
    fsq = new QlobberFSQ();

fsq.on('start', function ()
{
    var server = net.createServer().listen(parseInt(process.argv[2]));
    server.on('connection', function (c)
    {
        new MQlobberServer(fsq, c);
    });
});
```

Next, a program which connects to the server and subscribes to messages
published to a topic:

```javascript
// client_subscribe.js
var assert = require('assert'),
    MQlobberClient = require('mqlobber').MQlobberClient,
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
    s.on('end', function ()
    {
        console.log('received', info.topic, msg);
        assert.equal(msg, 'hello');
        c.end();
    });
});
```

Finally, a program which connects to the server and publishes a message to a
topic:

```javascript
// client_publish.js
var MQlobberClient = require('mqlobber').MQlobberClient,
    c = require('net').createConnection(parseInt(process.argv[2])),
    mq = new MQlobberClient(c);

mq.publish(process.argv[3], function ()
{
    c.end();
}).end('hello');
```

Run two servers listening on ports 8600 and 8601:

```shell
node server.js 8600 &
node server.js 8601 &
```

Subscribe to two topics, `foo.bar` and wildcard topic `foo.*`, one against each
server:

```shell
node client_subscribe.js 8600 foo.bar &
node client_subscribe.js 8601 'foo.*' &
```

Then publish a message to the topic `foo.bar`:

```shell
node client_publish.js 8600 foo.bar
```

You should see the following output, one line from each subscriber:

```
received foo.bar hello
received foo.bar hello
```

Only the servers should still be running and you can now terminate them:

```shell
$ jobs
[1]-  Running                 node server.js 8600 &
[2]+  Running                 node server.js 8601 &
$ kill %1 %2
[1]-  Terminated              node server.js 8600
[2]+  Terminated              node server.js 8601
```

## Installation

```shell
npm install mqlobber
```

## Licence

[MIT](LICENCE)

## Test

```shell
grunt test
```

## Lint

```shell
grunt lint
```

## Code Coverage

```shell
grunt converage
```

[Instanbul](http://gotwarlost.github.io/istanbul/) results are available [here](http://rawgit.davedoes.com/davedoesdev/mqlobber/master/coverage/lcov-report/index.html).

Coveralls page is [here](https://coveralls.io/r/davedoesdev/mqlobber).

#API

<a name="tableofcontents"></a>

- <a name="toc_mqlobberclientstream-options"></a>[MQlobberClient](#mqlobberclientstream-options)
- <a name="toc_mqlobberclientprototypesubscribetopic-handler-cb"></a><a name="toc_mqlobberclientprototype"></a>[MQlobberClient.prototype.subscribe](#mqlobberclientprototypesubscribetopic-handler-cb)
- <a name="toc_mqlobberclientprototypeunsubscribetopic-handler-cb"></a>[MQlobberClient.prototype.unsubscribe](#mqlobberclientprototypeunsubscribetopic-handler-cb)

## MQlobberClient(stream, [options])

> Create a new `MQlobberClient` object for publishing and subscribing to
messages.

**Parameters:**

- `{Duplex} stream` Connection to a server. The server should use [`MQlobberServer`](#mqlobberserver) on its side of the connection. How the
connection is made is up to the caller - it just has to supply a
[`stream.Duplex`](https://nodejs.org/dist/latest-v4.x/docs/api/stream.html#stream_class_stream_duplex). For example, [`net.Socket`](https://nodejs.org/dist/latest-v4.x/docs/api/net.html#net_class_net_socket) or [`PrimusDuplex`](https://github.com/davedoesdev/primus-backpressure#primusduplexmsg_stream-options).

- `{Object} [options]` Configuration options. This is passed down to [`QlobberDedup`](https://github.com/davedoesdev/qlobber#qlobberdedupoptions)
(which matches messages received from the server to handlers) and
[`BPMux`](https://github.com/davedoesdev/bpmux#bpmuxcarrrier-options)
(which multiplexes message streams over the connection to the
server). It also supports the following additional property:

  - `{Buffer} [handshake_data]` Application-specific handshake data to send to
    the server. The server-side [`MQlobberServer`](#mqlobberserver) object will
    emit this as a [`handshake`](#mqlobber_servereventshandshake) event to
    its application.

<sub>Go: [TOC](#tableofcontents)</sub>

<a name="mqlobberclientprototype"></a>

## MQlobberClient.prototype.subscribe(topic, handler, [cb])

> Subscribe to messages published to the server.

**Parameters:**

- `{String} topic` Which messages you're interested in receiving. Message topics are split into words using `.` as the separator. You can use `*` to match
exactly one word in a topic or `#` to match zero or more words. For example,
`foo.*` would match `foo.bar` whereas `foo.#` would match `foo`, `foo.bar` and
`foo.bar.wup`. Note these are the default separator and wildcard characters.
They can be changed on the server when [constructing the `QlobberFSQ` object]
(https://github.com/davedoesdev/qlobber#qlobberoptions) passed to
[`MQlobberServer`](#mqlobberserver).

- `{Function} handler` Function to call when a new message is received from the server due to its topic matching against `topic`. `handler` will be passed
the following arguments:

  - `{Readable} stream` The message content as a [Readable](http://nodejs.org/api/stream.html#stream_class_stream_readable). Note that _all_ subscribers will
    receive the same stream for each message.

  - `{Object} info` Metadata for the message, with the following properties:

    - `{String} topic` Topic to which the message was published.
    - `{Boolean} single` Whether this message is being given to at most one 
      handler (across all clients connected to all servers).
    - `{Integer} expires` When the message expires (number of seconds after
      1 January 1970 00:00:00 UTC).

- `{Function} [cb]` Optional function to call once the subscription has been registered with the server. This will be passed the following argument:

  - `{Object} err` If an error occurred then details of the error, otherwise `null`'.

<sub>Go: [TOC](#tableofcontents) | [MQlobberClient.prototype](#toc_mqlobberclientprototype)</sub>

## MQlobberClient.prototype.unsubscribe([topic], [handler], [cb])

> Unsubscribe to messages published to the server.

**Parameters:**

- `{String} [topic]` Which messages you're no longer interested in receiving via the `handler` function. This should be a topic you've previously passed to
[`subscribe`](#mqlobberprototypesubscribetopic-handler-cb). If topic is
`undefined` then all handlers for all topics are unsubscribed.

- `{Function} [handler]` The function you no longer want to be called with messages published to the topic `topic`. This should be a function you've
previously passed to [`subscribe`](#mqlobberprototypesubscribetopic-handler-cb).
If you subscribed `handler` to a different topic then it will still be called
for messages which match that topic. If `handler` is undefined, all handlers for
the topic `topic` are unsubscribed.

- `{Function]} [cb]` Optional function to call once `handler` has been unsubscribed from `topic`. This will be passed the following argument:

  - `{Object} err` If an error occurred then details of the error, otherwise `null`.

<sub>Go: [TOC](#tableofcontents) | [MQlobberClient.prototype](#toc_mqlobberclientprototype)</sub>

_&mdash;generated by [apidox](https://github.com/codeactual/apidox)&mdash;_
