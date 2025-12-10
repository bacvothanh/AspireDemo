using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;

namespace AspireDemo.API.Controllers
{
    public class ProduceController : Controller
    {
        private readonly IConfiguration _config;
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
        public async Task<IActionResult> Produce(string message)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = this._config.GetConnectionString("kafka")
            };

            using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();

            var topic1 = "kafka";
            await producer.ProduceAsync(topic1, new Message<Null, string> { Value = message });

            return Ok("Message sent!");
        }
    }
}
