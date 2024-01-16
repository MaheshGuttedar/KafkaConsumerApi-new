using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace KafkaConsumerApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class KafkaController : ControllerBase
    {
        private readonly ILogger<KafkaController> _logger;
        private readonly IConsumer<string, string> _consumer;

        public KafkaController(ILogger<KafkaController> logger)
        {
            _logger = logger;

            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092", // Replace with your Kafka broker(s)
                GroupId = "KafkaProducerApi",
                AutoOffsetReset = AutoOffsetReset.Earliest // Adjust this based on your needs
            };

            _consumer = new ConsumerBuilder<string, string>(config).Build();
            _consumer.Subscribe("test-topic");
        }

        [HttpGet("consume")]
        public IActionResult ConsumeMessage()
        {
            try
            {
                var result = _consumer.Consume(TimeSpan.FromSeconds(1)); // Adjust the timeout as needed

                if (result != null)
                {
                    _logger.LogInformation($"Consumed message: '{result.Message.Value}' from topic: '{result.Topic}'");
                    return Ok(result.Message.Value);
                }
                else
                {
                    return NoContent(); // No messages available
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error consuming message: {ex.Message}");
                return StatusCode(500, "Internal server error");
            }
        }
    }
}
