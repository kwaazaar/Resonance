using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Resonance.APIClient
{
    internal static class SerializationExtensions
    {
        private static JsonSerializerSettings SerializerSettings;

        static SerializationExtensions()
        {
            SerializerSettings = new JsonSerializerSettings
            {
                Formatting = Formatting.None,
                NullValueHandling = NullValueHandling.Ignore,
            };
        }

        public static StringContent ToStringContent(this object theObject)
        {
            return new StringContent(theObject.ToJson(), Encoding.UTF8, "application/json");
        }

        public static string ToJson(this object theObject)
        {
            return JsonConvert.SerializeObject(theObject, SerializerSettings);
        }

        public static T FromJson<T>(this string json)
        {
            return JsonConvert.DeserializeObject<T>(json, SerializerSettings);
        }
    }
}
