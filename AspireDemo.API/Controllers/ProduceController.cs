using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;

namespace AspireDemo.API.Controllers
{
    public class ProduceController : Controller
    {
        private readonly IConfiguration _config;
        private const string Topic = "kafka";
        public ProduceController(IConfiguration config)
        {
            _config = config;
        }

        [HttpGet("/")]
        public IActionResult Index()
        {
            return Ok("Hello world!");
        }

        [HttpGet("/produce")]
        public async Task<IActionResult> Produce(string message, string key)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = this._config.GetConnectionString("kafka")
            };

            using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

            await producer.ProduceAsync(Topic, new Message<string, string> { Value = message, Key = key });

            return Ok("Message sent!");
        }

        [HttpGet("/produce-multiple")]
        public async Task<IActionResult> ProduceMultiple(int amount)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = this._config.GetConnectionString("kafka")
            };
            using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

            for (int i = 0; i < amount; i++)
            {
                var message = $"Message {i}";
                var key = $"Key {i}";
                await producer.ProduceAsync(Topic, new Message<string, string> { Value = message, Key = key });
            }

            return Ok($"{amount} messages sent!");
        }
    }
}
