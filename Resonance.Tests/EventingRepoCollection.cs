using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Resonance.Tests
{
    [CollectionDefinition("EventingRepo")]
    public class EventingRepoCollection : ICollectionFixture<EventingRepoFactoryFixture>
    {
    }
}
