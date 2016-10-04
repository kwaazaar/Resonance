using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.PlatformAbstractions;

namespace Resonance.Tests
{
    public class BaseFixture
    {
        public IConfiguration Configuration { get; set; }

        public BaseFixture()
        {
            var builder = new ConfigurationBuilder()
                            .SetBasePath(PlatformServices.Default.Application.ApplicationBasePath)
                            //.SetBasePath(AppContext.BaseDirectory)
                            .AddJsonFile("xunit.runner.json", optional: false, reloadOnChange: false)
                            .AddEnvironmentVariables();
            var config = builder.Build();
            this.Configuration = config;
        }
    }
}
