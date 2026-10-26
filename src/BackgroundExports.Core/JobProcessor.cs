using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace BackgroundExports.Core;

/// <summary>
/// The worker logic, kept out of the hosted service so tests can drive it one step at a time.
/// </summary>
public sealed class JobProcessor(
    IDbContextFactory<ExportsDbContext> dbFactory,
    IEnumerable<IExportGenerator> generators,
    ExportOptions options,
    JobNotifier notifier,
    JobCancellation cancellation,
    TimeProvider clock,
    ILogger<JobProcessor> log)
{
    public static string ResultPath(ExportOptions o, Guid jobId) => Path.Combine(o.OutputDirectory, $"{jobId:N}.zip");

    private DateTime Now => clock.GetUtcNow().UtcDateTime;

    /// <summary>
    /// Jobs still marked Running at startup were interrupted by a crash or deploy. Put them back
    /// in the queue; the attempt they lost doesn't count against them.
    /// </summary>
    public async Task<int> RecoverInterruptedAsync(CancellationToken ct = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(ct);
        return await db.Jobs.Where(j => j.Status == JobStatus.Running)
            .ExecuteUpdateAsync(s => s
                .SetProperty(j => j.Status, JobStatus.Queued)
                .SetProperty(j => j.Attempts, j => j.Attempts - 1)
                .SetProperty(j => j.Progress, 0), ct);
    }

    /// <summary>Claims and runs one job. Returns false when there was nothing to do.</summary>
    public async Task<bool> RunNextAsync(CancellationToken stopping)
    {
        var job = await TryClaimAsync(stopping);
        if (job is null) return false;

        using var cts = cancellation.Register(job.Id, stopping);
        var temp = ResultPath(options, job.Id) + ".partial";
        try
        {
            var generator = generators.Single(g => g.ExportType == job.ExportType);
            var progress = new ProgressWriter(this, job.Id, cts);
            string fileName;
            await using (var file = File.Create(temp))
            {
                fileName = await generator.GenerateAsync(job, file, progress, cts.Token);
            }
            await progress.FlushAsync();
            cts.Token.ThrowIfCancellationRequested();

            File.Move(temp, ResultPath(options, job.Id), overwrite: true);
            await FinishAsync(job.Id, j =>
            {
                j.Status = JobStatus.Succeeded;
                j.Progress = 100;
                j.FileName = fileName;
                j.SizeBytes = new FileInfo(ResultPath(options, job.Id)).Length;
                j.ExpiresAt = Now + options.ResultLifetime;
                j.Error = null;
            });
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested)
        {
            // Shutting down: leave it Running. RecoverInterruptedAsync requeues it on the next start.
            log.LogInformation("Export {JobId} interrupted by shutdown", job.Id);
        }
        catch (OperationCanceledException)
        {
            await FinishAsync(job.Id, j => j.Status = JobStatus.Canceled);
        }
        catch (TransientExportException ex) when (job.Attempts < options.MaxAttempts)
        {
            var delay = options.RetryBaseDelay * Math.Pow(2, job.Attempts - 1);
            log.LogWarning("Export {JobId} attempt {Attempt} failed, retrying in {Delay}: {Error}", job.Id, job.Attempts, delay, ex.Message);
            await FinishAsync(job.Id, j =>
            {
                j.Status = JobStatus.Queued;
                j.NotBefore = Now + delay;
                j.Progress = 0;
                j.Error = $"Attempt {j.Attempts} failed: {ex.Message} Retrying.";
            });
        }
        catch (Exception ex)
        {
            log.LogError(ex, "Export {JobId} failed", job.Id);
            await FinishAsync(job.Id, j =>
            {
                j.Status = JobStatus.Failed;
                // Users see a safe summary; the details are in the log.
                j.Error = ex is TransientExportException
                    ? $"Failed after {j.Attempts} attempts: {ex.Message}"
                    : "The export failed. The error has been logged.";
            });
        }
        finally
        {
            cancellation.Unregister(job.Id);
            if (File.Exists(temp)) File.Delete(temp);
        }
        return true;
    }

    /// <summary>Deletes result files past their lifetime.</summary>
    public async Task<int> ExpireResultsAsync(CancellationToken ct = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(ct);
        var now = Now;
        var expired = await db.Jobs.Where(j => j.Status == JobStatus.Succeeded && j.ExpiresAt <= now).ToListAsync(ct);
        foreach (var job in expired)
        {
            File.Delete(ResultPath(options, job.Id));
            job.Status = JobStatus.Expired;
        }
        await db.SaveChangesAsync(ct);
        foreach (var job in expired) notifier.Publish(job.Id);
        return expired.Count;
    }

    private async Task<ExportJob?> TryClaimAsync(CancellationToken ct)
    {
        await using var db = await dbFactory.CreateDbContextAsync(ct);
        var now = Now;
        var job = await db.Jobs
            .Where(j => j.Status == JobStatus.Queued && (j.NotBefore == null || j.NotBefore <= now))
            .OrderBy(j => j.CreatedAt)
            .FirstOrDefaultAsync(ct);
        if (job is null) return null;

        job.Status = JobStatus.Running;
        job.Attempts++;
        job.StartedAt = now;
        job.ClaimToken = Guid.NewGuid();
        try
        {
            await db.SaveChangesAsync(ct);
        }
        catch (DbUpdateConcurrencyException)
        {
            return null; // another worker got it first; the caller loops and tries again
        }
        notifier.Publish(job.Id);
        return job;
    }

    private async Task FinishAsync(Guid jobId, Action<ExportJob> update)
    {
        // CancellationToken.None: recording the outcome must not be skipped because we're stopping.
        await using var db = await dbFactory.CreateDbContextAsync(CancellationToken.None);
        var job = await db.Jobs.SingleAsync(j => j.Id == jobId);
        update(job);
        if (!job.IsActive) job.CompletedAt = Now;
        await db.SaveChangesAsync(CancellationToken.None);
        notifier.Publish(jobId);
    }

    /// <summary>
    /// Saves progress at most once per 5%, and checks the database cancel flag each time,
    /// which is how a cancel from another server reaches this one.
    /// </summary>
    private sealed class ProgressWriter(JobProcessor owner, Guid jobId, CancellationTokenSource cts) : IProgress<int>
    {
        private int _saved;
        private Task _pending = Task.CompletedTask;

        public void Report(int value)
        {
            if (value - _saved < 5 && value < 100) return;
            _saved = value;
            // Chained so saves never overlap or land out of order.
            _pending = _pending.ContinueWith(_ => owner.SaveProgressAsync(jobId, value, cts)).Unwrap();
        }

        public Task FlushAsync() => _pending;
    }

    private async Task SaveProgressAsync(Guid jobId, int value, CancellationTokenSource cts)
    {
        await using var db = await dbFactory.CreateDbContextAsync();
        await db.Jobs.Where(j => j.Id == jobId).ExecuteUpdateAsync(s => s.SetProperty(j => j.Progress, value));
        if (await db.Jobs.Where(j => j.Id == jobId).Select(j => j.CancelRequested).SingleAsync()) cts.Cancel();
        notifier.Publish(jobId);
    }
}
