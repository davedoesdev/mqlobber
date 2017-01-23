/**
`handshake` event

Emitted by a `MQlobberClient` object after it successfully completes an initial
handshake with its peer `MQlobberServer` object on the server.

@param {Buffer} handshake_data Application-specific data which the
`MQlobberServer` object sent along with the handshake.
*/
MQlobberClient.events.handshake = function (handshake_data) {};

/**
`backoff` event

Emitted by a `MQlobberClient` object when it delays a request to the server
because the connection is at full capacity. If you want to avoid buffering
further requests, don't call [`subscribe`](#mqlobberclientprototypesubscribetopic-handler-cb),
[`unsubscribe`](#mqlobberclientprototypeunsubscribetopic-handler-cb) or
[`publish`](http://localhost:6419/#mqlobberclientprototypepublishtopic-options-cb) until a [`drain`](#mqlobberclienteventsdrain) event is emitted.
*/
MQlobberClient.events.backoff = function () {};

/**
`drain` event

Emitted by a `MQlobberClient` object when the multiplexing layer emits a [`drain`](https://github.com/davedoesdev/bpmux#bpmuxeventsdrain) event.
*/
MQlobberClient.events.drain = function () {};

/**
`full` event

Emitted by a `MQlobberClient` object when the multiplexing layer emits a [`full`](https://github.com/davedoesdev/bpmux#bpmuxeventsfull) event.
*/
MQlobberClient.events.full = function () {};

/**
`removed` event

Emitted by a `MQlobberClient` object when the multiplexing layer emits a [`removed`](https://github.com/davedoesdev/bpmux#bpmuxeventsremovedduplex) event.

@param {Duplex} duplex The multiplexed stream which has closed.
*/
MQlobberClient.events.removed = function (duplex) {};

/**
`error` event

Emitted by a `MQlobberClient` object if an error is emitted by the multiplexing
layer ([`bpmux`](https://github.com/davedoesdev/bpmux)), preventing proper
communication with the server. 

@param {Object} err The error that occurred.

@param {Object} obj The object on which the error occurred. 
*/
MQlobberClient.events.error = function (err, obj) {};

/**
`warning` event

Emmited by a `MQlobberClient` object when a recoverable error occurs. This will
usually be due to an error on an individual request or multiplexed stream.

Note that if there are no `warning` event listeners registered then the error
will be displayed using `console.error`.

@param {Object} err The error that occurred.

@param {Object} obj The object on which the error occurred. 
*/
MQlobberClient.events.warning = function (err, obj) {};
