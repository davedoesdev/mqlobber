/*
Take:

- stream
- optional handshake data for the control channel

Exposes:

- subscribe (topic, handler, cb)
- unsubscribe (topic, handler, cb)
- publish (topic, payload, options, cb)

Need to remember all active subscriptions so:

- don't send subscribe message if already subscribed
- can remove when unsubscribe all

We're going to need our own QlobberDedup to put our handlers on:

- We'll just get a topic back from the server
- Need to get list of handlers to call for the topic

This needs to be usable in browser.

*/

