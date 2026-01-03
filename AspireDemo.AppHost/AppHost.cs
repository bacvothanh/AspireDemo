var builder = DistributedApplication.CreateBuilder(args);

var kafka = builder.AddKafka("kafka")
    .WithKafkaUI();

builder.AddProject<Projects.AspireDemo_API>("aspiredemo-api").WithReplicas(2).WithReference(kafka).WaitFor(kafka);

builder.Build().Run();
