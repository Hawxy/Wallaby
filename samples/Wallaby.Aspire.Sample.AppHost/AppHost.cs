var builder = DistributedApplication.CreateBuilder(args);

var postgres = builder.AddPostgres("postgres").WithLogicalReplication();
var db = postgres.AddDatabase("app");

var meilisearch = builder.AddMeilisearch("meilisearch");

// Sample.WorkerApp reads ConnectionStrings:Postgres and Meilisearch:Host/ApiKey. Added by path, so the
// AppHost needs no project reference to it.
builder.AddDotnetProject("worker", "../Sample.WorkerApp/Sample.WorkerApp.csproj")
    .WithReference(db, connectionName: "Postgres")
    .WithEnvironment("Meilisearch__Host", meilisearch.Resource.UriExpression)
    .WithEnvironment("Meilisearch__ApiKey", meilisearch.Resource.MasterKeyParameter)
    .WaitFor(db)
    .WaitFor(meilisearch)
    .WithWallaby(db);

builder.Build().Run();
