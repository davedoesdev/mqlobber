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
