var builder = DistributedApplication.CreateBuilder(args);

var kafka = builder.AddKafka("kafka")
    .WithKafkaUI();

builder.AddProject<Projects.AspireDemo_API>("aspiredemo-api").WithReference(kafka);

builder.Build().Run();
