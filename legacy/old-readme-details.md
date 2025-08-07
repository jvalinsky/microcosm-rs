[Constellation](./constellation/)
--------------------------------------------

A global atproto backlink index ✨

- Self hostable: handles the full write throughput of the global atproto firehose on a raspberry pi 4b + single SSD
- Storage efficient: less than 2GB/day disk consumption indexing all references in all lexicons and all non-atproto URLs
- Handles record deletion, account de/re-activation, and account deletion, ensuring accurate link counts and respecting users data choices
- Simple JSON API

All social interactions in atproto tend to be represented by links (or references) between PDS records. This index can answer questions like "how many likes does a bsky post have", "who follows an account", "what are all the comments on a [frontpage](https://frontpage.fyi/) post", and more.

- **status**: works! api is unstable and likely to change, and no known instances have a full network backfill yet.
- source: [./constellation/](./constellation/)
- public instance: [constellation.microcosm.blue](https://constellation.microcosm.blue/)

_note: the public instance currently runs on a little raspberry pi in my house, feel free to use it! it comes with only with best-effort uptime, no commitment to not breaking the api for now, and possible rate-limiting. if you want to be nice you can put your project name and bsky username (or email) in your user-agent header for api requests._


App: Spacedust
--------------

A notification subscription service 💫

using the same "link source" concept as [constellation](./constellation/), offer webhook notifications for new references created to records

- **status**: in design


Library: [links](./links/)
------------------------------------

A rust crate (not published on crates.io yet) for optimistically parsing links out of arbitrary atproto PDS records, and potentially canonicalizing them

- **status**: unstable, might remain an internal lib for constellation (and spacedust, soon)
