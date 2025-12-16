using Confluent.Kafka;

namespace AspireDemo.API.Background
{
    public class KafkaConsumerService(IConsumer<string, string> consumer) : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            consumer.Subscribe("kafka");

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    // Use a short timeout to avoid a hard-blocking call
                    var deliveryResult = consumer.Consume(TimeSpan.FromMilliseconds(250));

                    if (deliveryResult is not null && deliveryResult.Message is not null)
                    {
                        Console.WriteLine(
                            $"Consumed message '{deliveryResult.Message.Value}' at: '{deliveryResult.TopicPartitionOffset}' at {DateTime.Now}.");

                        // handle the business logic here then commit the consumer when finished
                        //consumer.Commit(deliveryResult);
                    }

                    // Optional small yield to be nice to the scheduler
                    await Task.Yield();
                }
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown when stoppingToken is canceled
            }
            finally
            {
                try
                {
                    consumer.Close(); // Leave the group cleanly
                }
                catch
                {
                    // Ignore close errors during shutdown
                }

                consumer.Dispose();
            }
        }
    }
}