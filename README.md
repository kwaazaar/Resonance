# Resonance
Messaging library, supporting pub-sub, using MS SQL Server or MySql Server for storage.
Ideal for implementing a (business) event driven architecture, CQRS, pub-sub, microservices, etc.

**Core features:**
* Pub-sub (using topics and subscriptions)
* Subscriptions on multiple topics
* Fifo delivery (on publishing date), with priority-support to 'skip the line'
* Ordered delivery on functional key: this you want!
* Delayed delivery
* Message expiration (time to live), on both topic and subscriptions
* Max-deliveries (per subscription) to maximize the number of retries/redeliveries
* Filtered subscriptions (only matched messages go through)
 
## Topics and subscriptions ##
Messages are sent to *topics*. Whether each type of message gets its own topic or not is up to you. It may be usefull to group certain messages that are usually always processed by the same subscriber.
For processing these messages, a *subscription* must be created. A subscription subscribes to one or more topics. When messages are published to a topic, each subscription will get its own copy.

## Publishing a message ##
First we need to set-up the *EventPublisher* class. It's constructor requires an *IEventingRepoFactory*. In this example MS SQL Server is used, so we create a *MsSqlEventingRepoFactory* and provide a connectionstring:

    var connectionString = config.GetConnectionString("Resonance");
    var repoFactory = new MsSqlEventingRepoFactory(connectionString);
    var publisher = new EventPublisher(repoFactory);

Now we need to make sure the topic exists:

    var topic = publisher.GetTopicByName("Demo Topic");
    if (topic == null)
       topic = publisher.AddOrUpdateTopic(new Topic { Name = "Demo Topic" });

(to be continued)
