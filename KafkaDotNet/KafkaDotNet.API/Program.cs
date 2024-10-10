using KafkaDotNet.API;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddScoped<DataInferenceService>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();


app.MapGet("/event-producing", async (DataInferenceService producer, CancellationToken cancellationToken) =>
{

    await producer.ProduceAsync(cancellationToken);

    return "Event Sent!";
})
.WithOpenApi();

app.Run();