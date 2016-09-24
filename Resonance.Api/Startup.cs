using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Resonance.Repo;
using Resonance.Repo.Database;

namespace Resonance.Api
{
    public class Startup
    {
        public Startup(IHostingEnvironment env)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(env.ContentRootPath)
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddJsonFile($"appsettings.{env.EnvironmentName}.json", optional: true)
                .AddEnvironmentVariables();
            Configuration = builder.Build();
        }

        public IConfigurationRoot Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            // Add IConfiguration dependency (reason: allows access to config from any injected component)
            services.AddSingleton<IConfiguration>(Configuration);

            // Configure IEventingRepoFactory dependency (reason: the repo that must be used in this app)
            // To use MSSQLServer:
            var connectionString = Configuration.GetConnectionString("Resonance.MsSql");
            services.AddTransient<IEventingRepoFactory>((p) =>
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
            services.AddTransient<IEventPublisher, EventPublisher>();

            // Configure EventConsumer
            services.AddTransient<IEventConsumer, EventConsumer>();
            
            // Add framework services.
            services.AddMvc();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, ILoggerFactory loggerFactory)
        {
            loggerFactory.AddConsole(Configuration.GetSection("Logging"));
            loggerFactory.AddDebug();

            app.UseMvc();
        }
    }
}
