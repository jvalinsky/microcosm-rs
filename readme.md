microcosm
=========

This repo contains APIs and libraries for [atproto](https://atproto.com/) services from [microcosm](https://microcosm.blue):


🌌 [Constellation](./constellation/)
------------------------------------

A global atproto interactions backlink index as a simple JSON API. Works with every lexicon, runs on a raspberry pi, consumes less than 2GiB of disk per day. Handles record deletion, account de/re-activation, and account deletion, ensuring accurate link counts while respecting users' data choices.

- source: [./constellation/](./constellation/)
- [public instance + API docs](https://constellation.microcosm.blue/)
- status: used in production. APIs will change but backwards compatibility will be maintained as long as needed.


🎇 [Spacedust](./spacedust/)
----------------------------

A global atproto interactions firehose. Extracts all at-uris, DIDs, and URLs from every lexicon in the firehose, and exposes them over a websocket modelled after [jetstream](github.com/bluesky-social/jetstream).

- source: [./spacedust/](./spacedust/)
- [public instance + API docs](https://spacedust.microcosm.blue/)
- status: v0: the basics work and the APIs are in place! missing cursor replay, forward link storage, and delete event link hydration.

Demos:

- [Spacedust notifications](https://notifications.microcosm.blue/): web push notifications for _every_ atproto app
- [Zero-Bluesky real-time interaction-updating post embed](https://bsky.bad-example.com/zero-bluesky-realtime-embed/)


🛰️ [Slingshot](./slingshot)
---------------------------

A fast, eager, production-grade edge cache for atproto records and identities. Pre-caches all records from the firehose and maintains a longer-term cache of requested records on disk.

- source: [./slingshot/](./slingshot/)
- [public instance + API docs](https://slingshot.microcosm.blue/)
- status: v0: most XRPC APIs are working. cache storage is being reworked.


🛸 [UFOs API](./ufos)
---------------------

Timeseries stats and sample records for every [collection](https://atproto.com/guides/glossary#collection) ever seen in the atproto firehose. Unique users are counted in hyperloglog sketches enabling arbitrary cardinality aggregation across time buckets and/or NSIDs.

- source: [./ufos/](./ufos/)
- [public instance + API docs](https://ufos-api.microcosm.blue/)
- status: Used in production. It has APIs and they work! Needs improvement on indexing; needs more indexes and some more APIs to the data exposed.

See also: [UFOs atproto explorer](https://ufos.microcosm.blue/) built on UFOs API. ([source](github.com/at-microcosm/spacedust-utils))


💫 [Links](./links)
-------------------

Rust library for parsing and extracting links (at-uris, DIDs, and URLs) from atproto records.

- source: [./links/](./links/)
- status: not yet published to crates.io; needs some rework


🔭 Deprecated: [Who am I](./who-am-i)
-------------------------------------

An identity bridge for microcosm demos, that kinda worked. Fixing its problems is about equivalent to reinventing a lot of OIDC, so it's being retired.

- source: [./who-am-i/](./who-am-i/)
- status: ready for retirement.

Still in use for the Spacedust Notifications demo, but that will hopefully be migrated to use atproto oauth directly instead.
