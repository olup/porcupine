These are hand-authored transport contract fixtures, not captures from an SDK run.
They cover the supported capture object, Node-style batch envelope, and browser-style
array with properties.token. The placeholder is replaced with a fresh test project
key. SDK execution and pinned SDK versions are covered separately by the JavaScript
contract suite. Acceptance here establishes event transport only, not identity,
aliasing, feature flags, session recording, or complete PostHog compatibility.

`geoip-country.mmdb` is a synthetic MaxMind DB made with
`github.com/maxmind/mmdbwriter` v1.2.0. It has an IPv6 tree, 24-bit nodes, build
epoch 1, and database type `Porcupine-Test-Country`. It covers these records.

| Address | Record |
| --- | --- |
| `8.8.8.8` | `country.iso_code` US and `city.name` never-persist |
| `2606:4700::1111` | `country.iso_code` FR |
| `9.9.9.9` | Malformed `country.iso_code` (uint16 12) |
| `1.0.0.1` | `registered_country.iso_code` AU |
| `10.0.0.3` | `country.iso_code` AU (reserved networks enabled during generation) |
