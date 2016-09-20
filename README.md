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
The *EventPublisher* is used to publish messages and to manage topics. It's constructor requires an *IEventingRepoFactory*. In this example MS SQL Server is used, so we create a *MsSqlEventingRepoFactory* and provide a connectionstring:

    var connectionString = config.GetConnectionString("Resonance");
    var repoFactory = new MsSqlEventingRepoFactory(connectionString);
    var publisher = new EventPublisher(repoFactory);

Messages are published to topics, so before a message can be published, a topic must be created:

    var demoTopic = publisher.AddOrUpdateTopic(new Topic { Name = "Demo Topic" });

Now that the topic exists, publishing a message/event is very easy:

    publisher.Publish("Demo Topic", payload: "Hello there!");
    
Or to publish an object instead of a string:

    var order = new Order
    {
        Product = "Car",
        Price = 15000m,
        ItemCount = 1,
    };
    publisher.Publish<Order>("Demo Topic", payload: order);
    
## Consuming a message ##

The *EventConsumer* is used to consume messages and to manage subscriptions. Similar to the EventPublisher, it also takes a IEventingRepoFactory as an argument to its constructor:

    var consumer = new EventConsumer(repoFactory);

The message published above currently cannot be consumed, since there are no subscriptions. Creating a subscription is pretty straight forward:

    consumer.AddOrUpdateSubscription(new Subscription
    {
        Name = "Demo Subscription",
        TopicSubscriptions = new List<TopicSubscription>
        {
            new TopicSubscription { TopicId = demoTopic.Id, Enabled = true }
        }
    });

The subscription can receive messages from multiple topics. In this case it only receives messages from the above created *demoTopic*. If we publish the order once more, it can then be consumed from the subscription:

    publisher.Publish<Order>("Demo Topic", payload: order);
    var orderEvent = consumer.ConsumeNext<Order>("Demo Subscription").SingleOrDefault();

## Locking: the Visibility Timeout ##
If no messages are available, ConsumeNext will return null. If it *does* return a message (order in this case), the order can be processed, eg: by sending an invoice. However, if multiple eventconsumers are consuming events from the same subscription (which is a very valid scenario when many using an event driven architecture), other consumers should not receive the same order, since it would be processed more than once. This is the reason the message must be locked, before it can be consumed.

Locking is done by using a making the message invisible to other consumers, so that it will not be consumed more than once. The consumer gets a certain time to perform the processing. Once the message is processed, the consumer must explicitly *mark it consumed*:

    consumer.MarkConsumed(orderEvent.Id, orderEvent.DeliveryKey);
    
This will remove the event from the subscription-queue so that it cannot be consumed anymore. If the consumer does not process the message before the visibility timeout expires, the message becomes visible again and can be consumed again (*retried*).
The *delivery key* is generated on every 'lock' and must be provided when marking the message consumed as a way of prove the lock is yours to begin with.

The consumer decides how long the message must remain invisible. It can do so by providing the optional *visibilityTimeout* parameter to the ConsumeNext-method. By default it is set to 60 seconds. It's usually best to keep it rather large: if processing runs out of time, it can be very complex (if not impossible) to undo all processing done so far. And if it needs to be retried, it's rarely a problem to wait a while for the visibility timeout to expire.

If for some reason the message is unprocessable, eg: the publisher passed an incorrect payload to the event, it is useless to keep retrying. The message can then be marked *failed*:

    consumer.MarkFailed(orderEvent.Id, orderEvent.DeliveryKey, Reason.Other("Order data is missing!"));

## EventConsumptionWorker ##

A more convenient way to consume messages is to use the *EventConsumptionWorker*. It handles polling the subscription (with an exponential back-off mechanism), marks messages complete when done processing and handles parallel processing to maximize throughput.
Using the EventConsumptionWorker is not much harder than using the EventConsumer directly:

    var worker = new EventConsumptionWorker(consumer, "Demo Subscription",
        (ce) =>
        {
            // Process the event
            Console.WriteLine($"Event payload consumed: {ce.Payload}.");

            return ConsumeResult.Succeeded;
        });
    worker.Start();
    Console.WriteLine("Press a key to stop the worker...");
    Console.ReadKey();
    worker.Stop();

*Coming soon: advanced scenario's*
