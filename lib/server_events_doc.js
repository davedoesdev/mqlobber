/**
`subscribe_requested` event

Emitted by a `MQlobberServer` object when it receives a request from its peer
`MQlobberClient` object to subscribe to messages published to a topic.

If there are no listeners on this event, the default action is to call
[`subscribe(topic, cb)`](#mqlobberserver_subscribe). If you add a listener on
this event, the default action will _not_ be called. This gives you the
opportunity to filter subscription requests in the application.

@param {String} topic The topic to which the client is asking to subscribe.

@param {Function} cb Function to call after processing the subscription request.
This function _must_ be called even if you don't call
[`subscribe`](#mqlobberserver_subscribe) yourself. It takes a single argument:

  - `{Object} err` If `null` then a success status is returned to the client
    (whether you called [`subscribe`](#mqlobberserver_subscribe) or not).
    Otherwise, the client gets a failed status and a [`warning`](#mqlobbereventswarning) event is emitted with `err`.
*/
MQlobberServer.events.subscribe_requested = function (topic, cb) {};

/**
`unsubscribe_requested` event

Emitted by a `MQlobberServer` object when it receives a request from its peer
`MQlobberClient` object to unsubscribe from messages published to a topic.

If there are no listeners on this event, the default action is to call
[`unsubscribe(topic, cb)`](#mqlobberserver_unsubscribe). If you add a listener
on this event, the default action will _not_ be called. This gives you the
opportunity to filter unsubscription requestions in the application.

@param {String} topic The topic from which the client is asking to unsubscribe.

@param {Function} cb Function to call after processing the unsubscription
request. This function _must_ be called even if you don't call
[`unsubscribe`](#mqlobberserver_unsubscribe) yourself. It take a single
argument:

  - `{Object} err` If `null` then a success status is returned to the client
    (whether you called [`unsubscribe`](#mqlobberserver_unsubscribe) or not).
    Otherwise, the client gets a failed status and a [`warning`](#mqlobbereventswarning) event is emitted with `err`.
*/
MQlobberServer.events.unsubscribe_requested = function (topic, cb) {};

MQlobberServer.events.unsubscribe_all_requested
MQlobberServer.events.publish_requested
MQlobberServer.events.message

/**
`handshake` event

Emitted by a `MQlobberServer` object after it receives an initial handshake
message from its peer `MQlobberClient` object on the client.

@param {Buffer} handshake_data Application-specific data which the
`MQlobberClient` object sent along with the handshake.

@param {Function} delay_handshake By default, `MQlobberServer` replies to
`MQlobberClient`'s handshake message as soon as your event handler returns and
doesn't attach any application-specific handshake data. If you wish to delay
the handshake message or provide handshake data, call `delay_handshake`.
It returns another functon which you can call at any time to send the handshake
message. The returned function takes a single argument:

  - `{Buffer} [handshake_data]` Application-specific handshake data to send to
    the client. The client-side [`MQlobberClient`](#mqlobber_client) object will
    emit this as a [`handshake`](#mqlobber_clienteventshandshake) event to its
    application.
*/
MQlobberServer.events.handshake = function (handshake_data, delay_handshake) {};

/**
`backoff` event

Emitted by a `MQlobberServer` object when it delays a message to the client
because the connection is at full capacity.

If you want to avoid buffering further messages, use a `filter` function (see
[`QlobberFSQ`'s constructor](https://github.com/davedoesdev/qlobber-fsq#qlobberfsqoptions)) to prevent messages being sent until the connection `Duplex` emits a
[`drain`](https://nodejs.org/dist/latest-v4.x/docs/api/stream.html#stream_event_drain) event. In the `filter` function, a handler owned by a `MQlobberServer`
object will have a property named `mqlobber_stream` set to the connection
`Duplex`.

You can also use event listeners on [`subscribe_requested`](#mqlobberserver_eventssubscribe_requested), [`unsubscribe_requested`](#mqlobberserver_eventsunsubscribe_requested), [`unsubscribe_all_requested`](#mqlobberserver_eventsunsubscribe_all_requested) and [`publish_requested`](#mqlobberserver_publish_requested) to prevent responses being
sent to the client until the connection emits a `drain` event.

Depending on your application, you might also terminate the connection if it
can't keep up.
*/
MQlobberServer.events.backoff = function () {};

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

Emmited by a `MQlobberServer` object when a recoverable error occurs. This will
usually be due to an error on an individual request or multiplexed stream.

Note that if there are no `warning` event listeners registered then the error
will be displayed using `console.error`.

@param {Object} err The error that occurred.

@param {Object} obj The object on which the error occurred. 
*/
MQlobberServer.events.warning = function (err, obj) {};
