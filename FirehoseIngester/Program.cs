using Sync.Firehose;
using Sync.Runner;

Console.WriteLine("Hello, World!");

var runner = new MemoryRunner(async cursor =>
{
    // Persist the cursor to a database or file
    Console.WriteLine($"Cursor updated: {cursor}");
    await Task.Delay(50); // simulate other stuff happening
});

var firehoseOptions = new FirehoseOptions
{
    OnError = ex =>
    {
        Console.Error.WriteLine($"Error: {ex.Message}");
    },
    FilterCollections = ["app.bsky.feed.post"] // not implemented yet
};

var firehoseClient = new FirehoseClient(firehoseOptions);
await firehoseClient.StartAsync();

// Stop the firehose and runner when shutting down
await firehoseClient.StopAsync();
runner.Dispose();
