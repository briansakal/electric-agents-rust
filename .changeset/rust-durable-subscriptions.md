---
'@electric-ax/durable-streams-server-rust': minor
---

Add an optional persistent subscription control plane to the Rust Streams server,
including signed webhooks, pull-wake leases, generation fencing, and crash recovery
for Electric Agents backends. Enable it with the `subscriptions` Cargo feature and
the `--subscriptions` runtime flag.
