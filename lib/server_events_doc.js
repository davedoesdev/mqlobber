MQlobberServer.events.subscribe_requested
MQlobberServer.events.unsubscribe_requested
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

MQlobberServer.events.error
MQlobberServer.events.warning
