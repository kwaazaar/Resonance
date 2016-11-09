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
using Microsoft.Extensions.PlatformAbstractions;
using System.IO;

namespace Resonance.Web
{
    /// <summary>
    /// OWIN startup class
    /// </summary>
    public class Startup
    {
        /// <summary>
        /// Startup-constructor
        /// </summary>
        /// <param name="env"></param>
        public Startup(IHostingEnvironment env)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(env.ContentRootPath)
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddJsonFile($"appsettings.{env.EnvironmentName}.json", optional: true)
                .AddEnvironmentVariables();
            Configuration = builder.Build();
        }

        /// <summary>
        /// Configuration for the application
        /// </summary>
        public IConfigurationRoot Configuration { get; }

        /// <summary>
        /// This method gets called by the runtime. Use this method to add services to the container.
        /// </summary>
        /// <param name="services"></param>
        public void ConfigureServices(IServiceCollection services)
        {
            // Add IConfiguration dependency (reason: allows access to config from any injected component)
            services.AddSingleton<IConfiguration>(Configuration);

            // Configure IEventingRepoFactory dependency (reason: the repo that must be used in this app)
            // To use MSSQLServer:
            //var connectionString = Configuration.GetConnectionString("Resonance.MsSql");
            //services.AddTransient<IEventingRepoFactory>((p) =>
            //{
            //    return new MsSqlEventingRepoFactory(connectionString);
            //});
            // To use MySQL:
            var connectionString = Configuration.GetConnectionString("Resonance.MySql");
            services.AddTransient<IEventingRepoFactory>((p) =>
            {
                return new MySqlEventingRepoFactory(connectionString);
            });

            // Configure EventPublisher
            services.AddTransient<IEventPublisherAsync, EventPublisher>();

            // Configure EventConsumer
            services.AddTransient<IEventConsumerAsync, EventConsumer>();
            
            // Add framework services.
            services.AddMvc();

            // Enable generation of Swagger-Json
            services.AddSwaggerGen(options => {
                options.SingleApiVersion(new Swashbuckle.Swagger.Model.Info
                {
                    Version = "v1",
                    Title = "Resonance Api",
                    Description = "REST-based API for Resonance",
                    TermsOfService = "None"
                });
            });

            // Let SwaggerUI include XML-comments
            services.ConfigureSwaggerGen(c =>
            {
                c.IncludeXmlComments(Path.Combine(PlatformServices.Default.Application.ApplicationBasePath, "Resonance.Web.xml"));
            });
        }

        /// <summary>
        /// This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        /// </summary>
        /// <param name="app"></param>
        /// <param name="env"></param>
        /// <param name="loggerFactory"></param>
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, ILoggerFactory loggerFactory)
        {
            loggerFactory.AddConsole(Configuration.GetSection("Logging"));
            loggerFactory.AddDebug();

            // Enable Swagger, incl UI
            app.UseSwagger();
            app.UseSwaggerUi();

            app.UseMvc();
        }
    }
}
