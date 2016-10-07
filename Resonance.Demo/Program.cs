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
using System.Diagnostics;
using Microsoft.Extensions.PlatformAbstractions;

namespace Resonance.Demo
{
    public class Program
    {
        private static IServiceProvider serviceProvider;

        #region Payloads
        private static string payload100 = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";
        private static string payload2000 = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";
        #endregion

        public static void Main(string[] args)
        {
            var serviceCollection = new ServiceCollection();
            ConfigureServices(serviceCollection);
            serviceProvider = serviceCollection.BuildServiceProvider();

            var publisher = serviceProvider.GetRequiredService<IEventPublisher>();
            var consumer = serviceProvider.GetRequiredService<IEventConsumer>();

            // Make sure the topic exists
            var topic1 = publisher.GetTopicByName("Demo Topic 1") ??  publisher.AddOrUpdateTopic(new Topic { Name = "Demo Topic 1" });
            var topic2 = publisher.GetTopicByName("Demo Topic 2") ??  publisher.AddOrUpdateTopic(new Topic { Name = "Demo Topic 2" });
            var subscription1 = consumer.GetSubscriptionByName("Demo Subscription 1");
            if (subscription1 == null)
                subscription1 = consumer.AddOrUpdateSubscription(new Subscription
                {
                    Name = "Demo Subscription 1",
                    MaxDeliveries = 2,
                    Ordered = true,
                    TopicSubscriptions = new List<TopicSubscription>
                    {
                        new TopicSubscription
                        {
                            TopicId = topic1.Id.Value, Enabled = true,
                            Filtered = true,
                            Filters = new List<TopicSubscriptionFilter>
                            {
                                new TopicSubscriptionFilter { Header = "EventName", MatchExpression = "*" }, // Matches on every "EventName"-header, but header MUST exist!
                            },
                        },
                        new TopicSubscription { TopicId = topic2.Id.Value, Enabled = true },
                    }
                });
            var subscription2 = consumer.GetSubscriptionByName("Demo Subscription 2");
            if (subscription2 == null)
                subscription2 = consumer.AddOrUpdateSubscription(new Subscription
                {
                    Name = "Demo Subscription 2",
                    MaxDeliveries = 2,
                    Ordered = false,
                    TopicSubscriptions = new List<TopicSubscription>
                    {
                        new TopicSubscription { TopicId = topic1.Id.Value, Enabled = true },
                        new TopicSubscription { TopicId = topic2.Id.Value, Enabled = false }, // Not enabled
                    }
                });

            //var sw = new Stopwatch();
            //sw.Start();
            //int maxLoop = 10;
            //for (int i = 1; i <= maxLoop; i++)
            //{
            //    var iAsString = i.ToString();
            //    for (int fk = 1; fk <= 1000; fk++) // 1000 different functional keys, 4 TopicEvents per fk
            //    {
            //        var fkAsString = fk.ToString();
            //        publisher.Publish(topic1.Name, functionalKey: fkAsString, payload: payload100, headers: new Dictionary<string, string> { { "EventName", "Bla" } });
            //        publisher.Publish(topic1.Name, functionalKey: fkAsString, payload: payload2000); // Not delivered to sub1: EventName-header is missing
            //        publisher.Publish(topic2.Name, functionalKey: fkAsString, payload: payload2000); // Not delivered to sub2: topic2-subscription is not enabled
            //        publisher.Publish(topic2.Name, functionalKey: fkAsString, payload: payload100); // Not delivered to sub2: topic2-subscription is not enabled
            //    }
            //    Console.WriteLine($"Runs done: {i} of {maxLoop}");
            //}
            //sw.Stop();
            //Console.WriteLine($"Total time for publishing: {sw.Elapsed.TotalSeconds} sec");

            //var ce = consumer.ConsumeNext(subscription1.Name).FirstOrDefault();
            //if (ce != null)
            //    consumer.MarkConsumed(ce.Id, ce.DeliveryKey);

            var worker = new EventConsumptionWorker(consumer,
                "Demo Subscription 1", (ceW) =>
                {
                    //Console.WriteLine($"Consumed {ceW.Id} from thread {System.Threading.Thread.CurrentThread.ManagedThreadId}.");
                    return Task.FromResult<ConsumeResult>(DateTime.UtcNow.Millisecond == 1 ? ConsumeResult.Failed("sorry") : ConsumeResult.Succeeded);
                }, maxThreads: 10, minBackOffDelayInMs: 0,
                logger: serviceProvider.GetRequiredService<ILoggerFactory>().CreateLogger<EventConsumptionWorker>());
            worker.Start();
            Console.WriteLine("Press a key to stop the worker...");
            Console.ReadKey();
            worker.Stop();

            //consumer.DeleteSubscription(subscription1.Id);
            //publisher.DeleteTopic(topic1.Id, true);
        }

        private static void ConfigureServices(IServiceCollection serviceCollection)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(PlatformServices.Default.Application.ApplicationBasePath)
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
            //builder.AddEnvironmentVariables();
            var config = builder.Build();


            // Add IConfiguration dependency (reason: allows access to config from any injected component)
            serviceCollection.AddSingleton<IConfiguration>(config);

            ILoggerFactory loggerFactory = new LoggerFactory()
                .AddConsole(LogLevel.Trace);
            //.AddDebug(LogLevel.Trace);
            serviceCollection.AddSingleton<ILoggerFactory>(loggerFactory);

            // Configure IEventingRepoFactory dependency (reason: the repo that must be used in this app)

            // To use MSSQLServer:
            var connectionString = config.GetConnectionString("Resonance.MsSql");
            serviceCollection.AddTransient<IEventingRepoFactory>((p) =>
            {
                return new MsSqlEventingRepoFactory(connectionString);
            });

            // To use MySQL:
            //var connectionString = config.GetConnectionString("Resonance.MySql");
            //serviceCollection.AddTransient<IEventingRepoFactory>((p) =>
            //{
            //    return new MySqlEventingRepoFactory(connectionString);
            //});

            // Configure EventPublisher
            serviceCollection.AddTransient<IEventPublisher, EventPublisher>();

            // Configure EventConsumer
            serviceCollection.AddTransient<IEventConsumer, EventConsumer>();
        }
    }
}
