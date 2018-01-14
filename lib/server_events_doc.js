/**
`subscribe_requested` event

Emitted by a `MQlobberServer` object when it receives a request from its peer
`MQlobberClient` object to subscribe to messages published to a topic.

If there are no listeners on this event, the default action is to call
[`subscribe(topic, cb)`](#mqlobberserverprototypesubscribetopic-options-cb).
If you add a listener on this event, the default action will _not_ be called.
This gives you the opportunity to filter subscription requests in the
application.

@param {String} topic The topic to which the client is asking to subscribe.

@param {Function} cb Function to call after processing the subscription request.  This function _must_ be called even if you don't call [`subscribe`](#mqlobberserverprototypesubscribetopic-options-cb) yourself.  It takes the following arguments:
- `{Object} err` If `null` then a success status is returned to the client (whether you called [`subscribe`](#mqlobberserverprototypesubscribetopic-options-cb) or not). Otherwise, the client gets a failed status and a [`warning`](#mqlobberservereventswarningerr-obj) event is emitted with `err`.

- `{Integer} n` The number of subscriptions made.

- `{Buffer} [data]` Optional data to return to the client.
*/
MQlobberServer.events.subscribe_requested = function (topic, cb) {};

/**
`unsubscribe_requested` event

Emitted by a `MQlobberServer` object when it receives a request from its peer
`MQlobberClient` object to unsubscribe from messages published to a topic.

If there are no listeners on this event, the default action is to call
[`unsubscribe(topic, cb)`](#mqlobberserverprototypeunsubscribetopic-cb).
If you add a listener on this event, the default action will _not_ be called.
This gives you the opportunity to filter unsubscription requests in the
application.

@param {String} topic The topic from which the client is asking to unsubscribe.

@param {Function} cb Function to call after processing the unsubscription request. This function _must_ be called even if you don't call [`unsubscribe`](#mqlobberserverprototypeunsubscribetopic-cb) yourself. It takes the following arguments:
- `{Object} err` If `null` then a success status is returned to the client (whether you called [`unsubscribe`](#mqlobberserverprototypeunsubscribetopic-cb) or not). Otherwise, the client gets a failed status and a [`warning`](#mqlobberservereventswarningerr-obj) event is emitted with `err`.

- `{Integer} n` The number of subscriptions removed.

- `{Buffer} [data]` Optional data to return to the client.
*/
MQlobberServer.events.unsubscribe_requested = function (topic, cb) {};

/**
`unsubscribe_all_requested` event

Emited by a `MQlobberServer` object when it receives a request from its peer
`MQlobberClient` object to unsubscribe from all messages published to any topic.

If there are no listeners on this event, the default action is to call
[`unsubscribe(cb)`](#mqlobberserverprototypeunsubscribetopic-cb). If you add a listener
on this event, the default action will _not_ be called. This gives you the
opportunity to filter unsubscription requests in the application.

@param {Function} cb Function to call after processing the unsubscription request. This function _must_ be called even if you don't call [`unsubscribe`](#mqlobberserverprototypeunsubscribetopic-cb) yourself. It takes the following arguments:
- `{Object} err` If `null` then a success status is returned to the client (whether you called [`unsubscribe`](#mqlobberserverprototypeunsubscribetopic-cb) or not). Otherwise, the client gets a failed status and a [`warning`](#mqlobberservereventswarningerr-obj) event is emitted with `err`.

- `{Integer} n` The number of subscriptions removed.

- `{Buffer} [data]` Optional data to return to the client.
*/
MQlobberServer.events.unsubscribe_all_requested = function (cb) {};

/**
`publish_requested` event

Emitted by a `MQlobberServer` object when it receives a request from its peer
`MQlobberClient` object to publish a message to a topic.

If there are no listeners on this event, the default action is to call
`stream.pipe(fsq.publish(topic, options, cb))`, where `fsq` is the
[`QlobberFSQ`](https://github.com/davedoesdev/qlobber-fsq#qlobberfsqoptions)
instance you passed to `MQlobberServer`'s [constructor](#mqlobberserverfsq-stream-options).

@param {String} topic The topic to which the message should be published.

@param {Readable} stream The message data as a [`Readable`](https://nodejs.org/dist/latest-v4.x/docs/api/stream.html#stream_class_stream_readable). This is multiplexed over the connection to the client - back-pressure is applied to the sender `MQlobberClient` object according to when you call [`read`](https://nodejs.org/dist/latest-v4.x/docs/api/stream.html#stream_readable_read_size).

@param {Object} options Optional settings for this publication:
- `{Boolean} single` If `true` then the message should be published to _at most_ one client (across all servers). Otherwise, it should be published to all interested clients.
    
- `{Integer} ttl` Time-to-live (in seconds) for this message.
  
@param {Function} cb Function to call after processing the publication request. This function _must_ be called even if you don't call [`publish`](https://github.com/davedoesdev/qlobber-fsq#qlobberfsqprototypepublishtopic-payload-options-cb) yourself. It takes the following arguments:
- `{Object} err` If `null` then a success status is returned to the client (whether you called [`publish`](https://github.com/davedoesdev/qlobber-fsq#qlobberfsqprototypepublishtopic-payload-options-cb) or not). Otherwise, the client gets a failed status and a [`warning`](#mqlobberservereventswarningerr-obj) event is emitted with `err`.

- `{Buffer} [data]` Optional data to return to the client.
*/
MQlobberServer.events.publish_requested = function (topic, stream, options, cb) {};

/**
`message` event

Emitted by a `MQlobberServer` object when its `QlobberFSQ` object passes it a
message published to a topic to which its peer `MQlobberClient` object has subscribed.

If there are no listeners on this event, the default action is to call
`stream.pipe(multiplex())`.

You can add a listener on this event to insert processing between the message
stream and the client.

@param {Readable} stream The message content as a [Readable](http://nodejs.org/api/stream.html#stream_class_stream_readable). Note that _all_ subscribers will receive the same stream for each message.

@param {Object} info Metadata for the message, with the following properties:
- `{String} topic` Topic to which the message was published.
- `{Boolean} single` Whether this message is being given to _at most_ one handler (across all clients connected to all servers).
- `{Integer} expires` When the message expires (number of seconds after 1 January 1970 00:00:00 UTC). This is only present if the [`MQlobberServer`](#mqlobberserverfsq-stream-options) object was configured with `send_expires` set to `true`.
- `{Integer} size` Size of the message in bytes. This is only present if the server's [`MQlobberServer`](#mqlobberserverfsq-stream-options) instance is configured with `send_size` set to `true`.

@param {Function} multiplex Function to call in order to multiplex a new stream over the connection to the client. It returns the multiplexed stream, to which the data from `stream` should be written - after the application applies whatever transforms and processing it requires.

@param {Function} done If you don't call `multiplex` then you should call this function to indicate you have finished handling the message. `done` takes the following optional argument:
- `{Object} [err]` If an error occurred while handling the message, pass it here.
*/
MQlobberServer.events.message = function (stream, info, multiplex, done) {};

/**
`handshake` event

Emitted by a `MQlobberServer` object after it receives an initial handshake
message from its peer `MQlobberClient` object on the client.

@param {Buffer} handshake_data Application-specific data which the `MQlobberClient` object sent along with the handshake.

@param {Function} delay_handshake By default, `MQlobberServer` replies to `MQlobberClient`'s handshake message as soon as your event handler returns and doesn't attach any application-specific handshake data. If you wish to delay the handshake message or provide handshake data, call `delay_handshake`.  It returns another functon which you can call at any time to send the handshake message. The returned function takes a single argument:
- `{Buffer} [handshake_data]` Application-specific handshake data to send to the client. The client-side `MQlobberClient` object will emit this as a [`handshake`](#mqlobberclienteventshandshakehandshake_data) event to its application.
*/
MQlobberServer.events.handshake = function (handshake_data, delay_handshake) {};

/**
`backoff` event

Emitted by a `MQlobberServer` object when it delays a message to the client
because the connection is at full capacity.

If you want to avoid buffering further messages, use a `filter` function (see
`QlobberFSQ`'s [constructor](https://github.com/davedoesdev/qlobber-fsq#qlobberfsqoptions)) to prevent messages being sent until a [`drain`](#mqlobberservereventsdrain) event is emitted. In the `filter` function, a handler owned by a `MQlobberServer` object will have a property named `mqlobber_server` set to the `MQlobberServer` object.

You can also use event listeners on [`subscribe_requested`](#mqlobberservereventssubscribe_requestedtopic-cb), [`unsubscribe_requested`](#mqlobberservereventsunsubscribe_requestedtopic-cb), [`unsubscribe_all_requested`](#mqlobberservereventsunsubscribe_all_requestedcb) and [`publish_requested`](#mqlobberservereventspublish_requestedtopic-stream-options-cb) to prevent responses being
sent to the client until a `drain` event is emitted.

Depending on your application, you might also terminate the connection if it
can't keep up.
*/
MQlobberServer.events.backoff = function () {};

/**
`drain` event

Emitted by a `MQlobberServer` object when the multiplexing layer emits a [`drain`](https://github.com/davedoesdev/bpmux#bpmuxeventsdrain) event.
*/
MQlobberServer.events.drain = function () {};

/**
`full` event

Emitted by a `MQlobberServer` object when the multiplexing layer emits a [`full`](https://github.com/davedoesdev/bpmux#bpmuxeventsfull) event.
*/
MQlobberServer.events.full = function () {};

/**
`removed` event

Emitted by a `MQlobberServer` object when the multiplexing layer emits a [`removed`](https://github.com/davedoesdev/bpmux#bpmuxeventsremovedduplex) event.

@param {Duplex} duplex The multiplexed stream which has closed.
*/
MQlobberServer.events.removed = function (duplex) {};

/**
`ack` event

Emitted by a `MQlobberServer` object when the client has acknowledged receipt
of a message.

@param {Object} info Metadata for the message, with the following properties:
- `{String} topic` Topic to which the message was published.
- `{Boolean} single` Always `true` because acknowledgements are only supported for messages which were given to a single handler (across all clients connected to all servers).
- `{Integer} expires` When the message expires (number of milliseconds after 1 January 1970 00:00:00 UTC).
*/
MQlobberServer.events.ack = function (info) {};

/**
`error` event

Emitted by a `MQlobberServer` object if an error is emitted by the multiplexing
layer ([`bpmux`](https://github.com/davedoesdev/bpmux)), preventing proper
communication with the client. 

@param {Object} err The error that occurred.

@param {Object} obj The object on which the error occurred. 
*/
MQlobberServer.events.error = function (err, obj) {};

/**
`warning` event

Emited by a `MQlobberServer` object when a recoverable error occurs. This will
usually be due to an error on an individual request or multiplexed stream.

Note that if there are no `warning` event listeners registered then the error
will be displayed using `console.error`.

@param {Object} err The error that occurred.

@param {Object} obj The object on which the error occurred. 
*/
MQlobberServer.events.warning = function (err, obj) {};
