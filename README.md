# Resonance
Eventing library, with support for MsSql and MySql.
Ideal for implementing CQRS, business events, pub-sub, microservices, etc.

Supported features:
* Pub-sub
* Subscriptions on multiple topics
* Ordered delivery (now on publicationdate, on functional key coming soon)
* Priority support (higher priority delivered first)
* Delayed delivery
* Message expiration (time to live), both topic and Subscriptions
* Max-deliveries (per subscription)
* Filtered subscriptions (only matched messages go through)
