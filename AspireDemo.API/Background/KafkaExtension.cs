using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace AspireDemo.API.Background
{
    public static class KafkaExtension
    {
        public static async Task CreateKafkaTopicAsync(
    string bootstrapServers,
    string topicName,
    int numPartitions = 3,
    short replicationFactor = 1)
        {
            var config = new AdminClientConfig
            {
                BootstrapServers = bootstrapServers
            };

            using var adminClient = new AdminClientBuilder(config).Build();

            var topicSpecification = new TopicSpecification
            {
                Name = topicName,
                NumPartitions = numPartitions,
                ReplicationFactor = replicationFactor
            };

            try
            {
                await adminClient.CreateTopicsAsync(new[] { topicSpecification });
                Console.WriteLine($"Topic '{topicName}' created successfully!");
            }
            catch (CreateTopicsException ex)
            {
                if (ex.Results[0].Error.Code == ErrorCode.TopicAlreadyExists)
                {
                    Console.WriteLine($"Topic '{topicName}' already exists.");
                }
                else
                {
                    Console.WriteLine($"Error creating topic: {ex.Results[0].Error.Reason}");
                    throw;
                }
            }
        }
    }
}
