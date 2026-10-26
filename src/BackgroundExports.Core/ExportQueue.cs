using Microsoft.EntityFrameworkCore;

namespace BackgroundExports.Core;

/// <summary>
/// What the web app calls. Requesting an export only writes a row and returns, so the
/// request finishes in milliseconds no matter how big the export is.
/// </summary>
public sealed class ExportQueue(
    IDbContextFactory<ExportsDbContext> dbFactory,
    IEnumerable<IExportGenerator> generators,
    ExportOptions options,
    JobSignal signal,
    JobNotifier notifier,
    JobCancellation cancellation,
    TimeProvider clock)
{
    public IReadOnlyList<string> ExportTypes { get; } = generators.Select(g => g.ExportType).ToList();

    public async Task<ExportJob> EnqueueAsync(string user, string exportType, int rowCount, CancellationToken ct = default)
    {
        if (!ExportTypes.Contains(exportType)) throw new ExportRequestException($"Unknown export type “{exportType}”.");
        if (rowCount < 1 || rowCount > options.MaxRows) throw new ExportRequestException($"Row count must be between 1 and {options.MaxRows:N0}.");

        await using var db = await dbFactory.CreateDbContextAsync(ct);
        var active = await db.Jobs.CountAsync(j => j.RequestedBy == user && (j.Status == JobStatus.Queued || j.Status == JobStatus.Running), ct);
        if (active >= options.MaxActivePerUser)
            throw new ExportRequestException($"You already have {active} exports in progress. Wait for one to finish or cancel one.");

        var job = new ExportJob
        {
            Id = Guid.NewGuid(),
            RequestedBy = user,
            ExportType = exportType,
            RowCount = rowCount,
            Status = JobStatus.Queued,
            CreatedAt = clock.GetUtcNow().UtcDateTime,
            ClaimToken = Guid.NewGuid(),
        };
        db.Jobs.Add(job);
        await db.SaveChangesAsync(ct);
        signal.Notify();
        notifier.Publish(job.Id);
        return job;
    }

    public async Task CancelAsync(Guid jobId, string user, CancellationToken ct = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(ct);
        var job = await db.Jobs.SingleOrDefaultAsync(j => j.Id == jobId && j.RequestedBy == user, ct);
        if (job is null || !job.IsActive) return;

        if (job.Status == JobStatus.Queued)
        {
            job.Status = JobStatus.Canceled;
            job.CompletedAt = clock.GetUtcNow().UtcDateTime;
            // Bump the token so a worker that read this job a moment ago can't claim it.
            job.ClaimToken = Guid.NewGuid();
        }
        else
        {
            job.CancelRequested = true;
        }

        try
        {
            await db.SaveChangesAsync(ct);
        }
        catch (DbUpdateConcurrencyException)
        {
            // A worker claimed it between our read and save; ask the running job to stop instead.
            await db.Jobs.Where(j => j.Id == jobId).ExecuteUpdateAsync(s => s.SetProperty(j => j.CancelRequested, true), ct);
        }
        cancellation.Cancel(jobId);
        notifier.Publish(jobId);
    }

    public async Task<List<ExportJob>> ListAsync(string user, CancellationToken ct = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(ct);
        return await db.Jobs.AsNoTracking().Where(j => j.RequestedBy == user)
            .OrderByDescending(j => j.CreatedAt).Take(50).ToListAsync(ct);
    }

    /// <summary>Only the person who requested an export can download it, and only until it expires.</summary>
    public async Task<(string FileName, Stream Content)?> OpenResultAsync(Guid jobId, string user, CancellationToken ct = default)
    {
        await using var db = await dbFactory.CreateDbContextAsync(ct);
        var job = await db.Jobs.AsNoTracking().SingleOrDefaultAsync(j => j.Id == jobId && j.RequestedBy == user, ct);
        if (job is not { Status: JobStatus.Succeeded } || job.ExpiresAt <= clock.GetUtcNow().UtcDateTime) return null;
        var path = JobProcessor.ResultPath(options, job.Id);
        return File.Exists(path) ? (job.FileName!, File.OpenRead(path)) : null;
    }
}
