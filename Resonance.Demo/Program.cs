using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using System.IO;
using Resonance.Repo;
using Resonance.Models;
using Resonance.Repo.Database;
using Microsoft.Extensions.Logging;

namespace Resonance.Demo
{
    public class Program
    {
        private static IServiceProvider serviceProvider;

        public static void Main(string[] args)
        {
            var serviceCollection = new ServiceCollection();
            ConfigureServices(serviceCollection);
            serviceProvider = serviceCollection.BuildServiceProvider();

            var publisher = serviceProvider.GetRequiredService<IEventPublisher>();
            var consumer = serviceProvider.GetRequiredService<IEventConsumer>();

            // Make sure the topic exists
            var topic = publisher.GetTopicByName("Demo Topic");
            if (topic == null)
                topic = publisher.AddOrUpdateTopic(new Topic
                {
                    Name = "Demo Topic",
                    Notes = "This topic is for demo purposes. Nothing to see here, move along!",
                });
            var subscription = consumer.GetSubscriptionByName("Demo Subscription");
            if (subscription == null)
                subscription = consumer.AddOrUpdateSubscription(new Subscription
                {
                    Name = "Demo Subscription",
                    DeliveryDelay = 0,
                    MaxDeliveries = 2,
                    Ordered = true,
                    TimeToLive = 600,
                    TopicSubscriptions = new List<TopicSubscription>
                    {
                        new TopicSubscription
                        {
                            TopicId = topic.Id,
                            Enabled = true,
                        },
                    },
                });

            // Now publish an event
            publisher.Publish(
                topicName: "Demo Topic",
                headers: new Dictionary<string, string> { { "EventName", "PaymentReceived" }, { "MessageId", "12345" } },
                functionalKey: "1234",
                payload: new Tuple<string, int, string>("Robert", 40, "Holland")); // Publish typed

            var worker = new EventConsumptionWorker(consumer,
                serviceProvider.GetRequiredService<ILoggerFactory>().CreateLogger<EventConsumptionWorker>(),
                "Demo Subscription",
                100, 5000, 30,
                (ce) =>
                {
                    if (DateTime.UtcNow.Second % 2 == 0)
                        return ConsumeResult.Succeeded;
                    else
                        return ConsumeResult.MustSuspend(TimeSpan.FromSeconds(10), "Zomaar :)");
                });
            worker.Start();
            Console.WriteLine("Press a key to stop the worker...");
            Console.ReadKey();
            worker.Stop();

            //var consEvent = consumer.ConsumeNext<Tuple<string, int, string>>("Demo Subscription"); // Consume typed
            //if (consEvent != null)
            //    consumer.MarkConsumed(consEvent.Id, consEvent.DeliveryKey);
            //    //consumer.MarkFailed(consEvent.Id, consEvent.DeliveryKey, Reason.Other("Kaput"));

            consumer.DeleteSubscription(subscription.Id);
            publisher.DeleteTopic(topic.Id, true);
        }

        private static void ConfigureServices(IServiceCollection serviceCollection)
        {
            var builder = new ConfigurationBuilder()
                //.SetBasePath(PlatformServices.Default.Application.ApplicationBasePath)
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
            //builder.AddEnvironmentVariables();
            var config = builder.Build();


            // Add IConfiguration dependency (reason: allows access to config from any injected component)
            serviceCollection.AddSingleton<IConfiguration>(config);

            ILoggerFactory loggerFactory = new LoggerFactory()
                .AddConsole()
                .AddDebug();
            serviceCollection.AddSingleton<ILoggerFactory>(loggerFactory);

            // Configure IDbConnection dependency (reason: may be required by IEventingRepo dependencies)
            var connectionString = config.GetConnectionString("Resonance");
            serviceCollection.AddTransient<IDbConnection>((p) => {
                return new SqlConnection(connectionString);
            });

            // Configure IEventingRepo dependency (reason: the repo that must be used in this app)
            serviceCollection.AddTransient<IEventingRepo, MsSqlEventingRepo>();

            // Configure EventPublisher
            serviceCollection.AddTransient<IEventPublisher, EventPublisher>();

            // Configure EventConsumer
            serviceCollection.AddTransient<IEventConsumer, EventConsumer>();
        }
    }
}
