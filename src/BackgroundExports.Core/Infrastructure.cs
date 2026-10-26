using System.Collections.Concurrent;

namespace BackgroundExports.Core;

public sealed class ExportOptions
{
    /// <summary>How many exports one person can have queued or running at once.</summary>
    public int MaxActivePerUser { get; set; } = 3;
    public int MaxAttempts { get; set; } = 3;
    public TimeSpan RetryBaseDelay { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan ResultLifetime { get; set; } = TimeSpan.FromHours(24);
    public int MaxRows { get; set; } = 1_000_000;
    public required string OutputDirectory { get; set; }
}

/// <summary>Wakes idle workers as soon as a job is queued instead of waiting for the next poll.</summary>
public sealed class JobSignal
{
    private readonly SemaphoreSlim _signal = new(0);

    public void Notify() => _signal.Release();

    public async Task WaitAsync(TimeSpan timeout, CancellationToken ct)
    {
        try { await _signal.WaitAsync(timeout, ct); }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
    }
}

/// <summary>Raised whenever a job changes so open pages can refresh. In-process only.</summary>
public sealed class JobNotifier
{
    public event Action<Guid>? JobChanged;
    public void Publish(Guid jobId) => JobChanged?.Invoke(jobId);
}

/// <summary>
/// Cancellation for jobs running in this process. A user's cancel trips the token right away;
/// the processor also checks the database flag, which covers a job running on another server.
/// </summary>
public sealed class JobCancellation
{
    private readonly ConcurrentDictionary<Guid, CancellationTokenSource> _running = new();

    public CancellationTokenSource Register(Guid jobId, CancellationToken stopping)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(stopping);
        _running[jobId] = cts;
        return cts;
    }

    public void Unregister(Guid jobId)
    {
        if (_running.TryRemove(jobId, out var cts)) cts.Dispose();
    }

    public void Cancel(Guid jobId)
    {
        if (_running.TryGetValue(jobId, out var cts)) cts.Cancel();
    }
}

/// <summary>A failure worth retrying: a timeout, a dropped connection, a busy source system.</summary>
public sealed class TransientExportException(string message) : Exception(message);

public class ExportRequestException(string message) : Exception(message);
