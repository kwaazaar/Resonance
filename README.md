# Resonance
Eventing library, with support for MsSql (MySql coming soon).
Ideal for implementing CQRS, business events, pub-sub, etc.

Supported features:
* Pub-sub
* Subscriptions on multiple topics
* Ordered delivery (now on publicationdate, on functional key coming soon)
* Delayed delivery
* Message expiration (time to live), both topic and Subscriptions
* Max-deliveries (per subscription)
* Filtered subscriptions (only matched messages go through)
