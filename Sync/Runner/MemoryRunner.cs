namespace Sync.Runner;

using System.Collections.Concurrent;

public class MemoryRunner(Func<string, Task>? setCursor = null)
{
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _semaphores = new();

    public async Task ProcessEventAsync(string repoId, Func<Task> handleEvent)
    {
        var semaphore = _semaphores.GetOrAdd(repoId, _ => new SemaphoreSlim(1, 1));

        await semaphore.WaitAsync();
        try
        {
            await handleEvent();

            // Optionally persist the cursor after processing
            if (setCursor != null)
            {
                await setCursor(repoId); // Pass last known cursor or ID
            }
        }
        finally
        {
            semaphore.Release();
        }
    }

    public void Dispose()
    {
        foreach (var semaphore in _semaphores.Values)
        {
            semaphore.Dispose();
        }
    }
}
