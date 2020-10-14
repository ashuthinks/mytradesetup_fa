using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using StackExchange.Redis.Extensions.Core.Abstractions;
using StackExchange.Redis.Extensions.Core.Configuration;
using StackExchange.Redis.Extensions.Core.Models;
using StackExchange.Redis.Extensions.Newtonsoft;

namespace readrediscachefa
{
    public static class Function1
    {
        // redis cache
        static NewtonsoftSerializer serializer = new NewtonsoftSerializer();
        static SinglePoolRedisCacheConnectionPoolManager spRedisCacheConnectionPoolMgr = new SinglePoolRedisCacheConnectionPoolManager(Environment.GetEnvironmentVariable("RedisConnectionString"));
        static RedisConfiguration redisConfiguration = new RedisConfiguration();
        static IRedisDatabase cacheClient = new RedisCacheClient(spRedisCacheConnectionPoolMgr, serializer, redisConfiguration).Db0;

        [FunctionName("Function1")]
        public static async System.Threading.Tasks.Task RunAsync([TimerTrigger("0 */1 * * * *")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            var cachedTokens = await cacheClient.GetAsync<List<MyObject>>("tickData");

            var resultSet = cachedTokens.GroupBy(i => i.GetStartOfPeriodByMins(1))
               .Select(gr =>
              new
              {
                  StartOfPeriod = gr.Key,
                  Min = gr.Min(item => item.Low),
                  Max = gr.Max(item => item.High),
                  Open = gr.OrderBy(item => item.TimeStamp).First().Open,
                  Close = gr.OrderBy(item => item.TimeStamp).Last().Close
              });

            var my5min = resultSet.LastOrDefault();

            log.LogInformation("time " + my5min.StartOfPeriod + " open " + my5min.Open + " high " + my5min.Max + " low " + my5min.Min + " close " + my5min.Close);
        }
    }

    public class MyObject
    {
        public uint InstrumentID { get; set; }
        public decimal Close { get; set; }
        public decimal High { get; set; }
        public decimal Low { get; set; }
        public decimal Open { get; set; }
        public DateTime TimeStamp { get; set; }
        public uint Volume { get; set; }

        public DateTime Created { get; } = DateTime.Now;
        public DateTime Ttl { get; } = DateTime.Now.AddMinutes(1);
        public DateTime? Persisted { get; set; }

        public bool IsDead => DateTime.Now > Ttl;
        public bool IsPersisted => Persisted.HasValue;
        public bool TimeToPersist => IsPersisted == false && DateTime.Now > Created.AddMinutes(1);

        public DateTime GetStartOfPeriodByMins(int numMinutes)
        {
            int oldMinutes = TimeStamp.Minute;
            int newMinutes = (oldMinutes / numMinutes) * numMinutes;

            DateTime startOfPeriod = new DateTime(TimeStamp.Year, TimeStamp.Month, TimeStamp.Day, TimeStamp.Hour, newMinutes, 0);

            return startOfPeriod;
        }
    }
    public class SinglePoolRedisCacheConnectionPoolManager : IRedisCacheConnectionPoolManager
    {
        private readonly string connectionString;

        public SinglePoolRedisCacheConnectionPoolManager(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public void Dispose()
        {

        }

        public IConnectionMultiplexer GetConnection()
        {
            return ConnectionMultiplexer.Connect(connectionString);
        }

        public ConnectionPoolInformation GetConnectionInformations()
        {
            throw new NotImplementedException();
        }
    }
}
