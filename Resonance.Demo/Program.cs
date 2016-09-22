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
            var topic1 = publisher.AddOrUpdateTopic(new Topic { Id = "c526527a-9d16-4fa5-8ed7-000000000001", Name = "Demo Topic 1" });
            var topic2 = publisher.AddOrUpdateTopic(new Topic { Id = "c526527a-9d16-4fa5-8ed7-000000000002", Name = "Demo Topic 2" });
            var topic3 = publisher.AddOrUpdateTopic(new Topic { Id = "c526527a-9d16-4fa5-8ed7-000000000003", Name = "Demo Topic 3" });
            var topic4 = publisher.AddOrUpdateTopic(new Topic { Id = "c526527a-9d16-4fa5-8ed7-000000000004", Name = "Demo Topic 4" });
            var topic5 = publisher.AddOrUpdateTopic(new Topic { Id = "c526527a-9d16-4fa5-8ed7-000000000005", Name = "Demo Topic 5" });
            var subscription1 = consumer.AddOrUpdateSubscription(new Subscription
            {
                Id = "a526527a-9d16-4fa5-8ed7-000000000001",
                Name = "Demo Subscription 1",
                MaxDeliveries = 2,
                Ordered = true,
                TopicSubscriptions = new List<TopicSubscription>
                { new TopicSubscription { TopicId = topic1.Id, Enabled = true,
                Filters = new List<TopicSubscriptionFilter>
                {
                    new TopicSubscriptionFilter { Header = "EventName", MatchExpression = "*" },
                }, Filtered = true
                }, new TopicSubscription { TopicId = topic3.Id, Enabled = true }, new TopicSubscription { TopicId = topic5.Id, Enabled = true } }
            });
            var subscription2 = consumer.AddOrUpdateSubscription(new Subscription
            {
                Id = "a526527a-9d16-4fa5-8ed7-000000000002",
                Name = "Demo Subscription 2",
                MaxDeliveries = 2,
                Ordered = false,
                TopicSubscriptions = new List<TopicSubscription>
                { new TopicSubscription { TopicId = topic1.Id, Enabled = true }, new TopicSubscription { TopicId = topic2.Id, Enabled = true }, new TopicSubscription { TopicId = topic4.Id, Enabled = true } }
            });

            //var sw = new Stopwatch();
            //sw.Start();
            //for (int i = 0; i < 100; i++)
            //{
            //    var iAsString = i.ToString();
            //    for (int fk = 0; fk < 1000; fk++)
            //    {
            //        var fkAsString = fk.ToString();
            //        publisher.Publish(topic1.Name, functionalKey: fkAsString, payload: payload100, headers: new Dictionary<string, string> { { "EventName", "Bla" } });
            //        publisher.Publish(topic1.Name, functionalKey: fkAsString, payload: payload2000);
            //        publisher.Publish(topic2.Name, functionalKey: fkAsString, payload: payload100);
            //        publisher.Publish(topic2.Name, functionalKey: fkAsString, payload: payload2000);
            //        publisher.Publish(topic3.Name, functionalKey: fkAsString, payload: payload100);
            //        publisher.Publish(topic4.Name, functionalKey: fkAsString, payload: payload100);
            //        publisher.Publish(topic5.Name, functionalKey: fkAsString, payload: payload100);
            //    }
            //    Console.WriteLine($"Run done: {i}");
            //}
            //sw.Stop();
            //Console.WriteLine($"Total time for publishing: {sw.Elapsed.TotalSeconds} sec");

            var worker = new EventConsumptionWorker(consumer,
                "Demo Subscription 1", (ce) =>
                {
                    //Console.WriteLine($"Consumed {ce.Id} from thread {System.Threading.Thread.CurrentThread.ManagedThreadId}.");
                    return DateTime.UtcNow.Millisecond == 1 ? ConsumeResult.Failed("sorry") : ConsumeResult.Succeeded;
                }, maxThreads: 100, minBackOffDelayInMs: 0,
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
                //.SetBasePath(PlatformServices.Default.Application.ApplicationBasePath)
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
            //builder.AddEnvironmentVariables();
            var config = builder.Build();


            // Add IConfiguration dependency (reason: allows access to config from any injected component)
            serviceCollection.AddSingleton<IConfiguration>(config);

            ILoggerFactory loggerFactory = new LoggerFactory()
                .AddConsole(LogLevel.Information);
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
