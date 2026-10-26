using Microsoft.EntityFrameworkCore;

namespace BackgroundExports.Core;

public enum JobStatus { Queued, Running, Succeeded, Failed, Canceled, Expired }

/// <summary>
/// A requested export. The database row is the source of truth for the queue: if the app
/// restarts mid-export, nothing is lost, because the job is still here and still says what to do.
/// </summary>
public class ExportJob
{
    public Guid Id { get; set; }
    public required string RequestedBy { get; set; }
    public required string ExportType { get; set; }
    public int RowCount { get; set; }

    public JobStatus Status { get; set; }
    public int Progress { get; set; }
    public int Attempts { get; set; }
    public string? Error { get; set; }
    public bool CancelRequested { get; set; }

    public DateTime CreatedAt { get; set; }
    public DateTime? StartedAt { get; set; }
    public DateTime? CompletedAt { get; set; }

    /// <summary>A retry waits until this time (exponential backoff).</summary>
    public DateTime? NotBefore { get; set; }

    public string? FileName { get; set; }
    public long? SizeBytes { get; set; }
    public DateTime? ExpiresAt { get; set; }

    /// <summary>
    /// Changes on every claim. Two workers (or two servers) that read the same queued job can't
    /// both start it: the second one's save fails the concurrency check.
    /// </summary>
    public Guid ClaimToken { get; set; }

    public bool IsActive => Status is JobStatus.Queued or JobStatus.Running;
}

public class ExportsDbContext(DbContextOptions<ExportsDbContext> options) : DbContext(options)
{
    public DbSet<ExportJob> Jobs => Set<ExportJob>();

    protected override void OnModelCreating(ModelBuilder b)
    {
        b.Entity<ExportJob>(e =>
        {
            e.Property(x => x.RequestedBy).HasMaxLength(100);
            e.Property(x => x.ExportType).HasMaxLength(50);
            e.Property(x => x.Status).HasConversion<string>().HasMaxLength(10);
            e.Property(x => x.Error).HasMaxLength(1000);
            e.Property(x => x.FileName).HasMaxLength(200);
            e.Property(x => x.ClaimToken).IsConcurrencyToken();
            e.Ignore(x => x.IsActive);
            // The worker's "next job" query and the per-user "my exports" list.
            e.HasIndex(x => new { x.Status, x.NotBefore, x.CreatedAt });
            e.HasIndex(x => new { x.RequestedBy, x.CreatedAt });
        });
    }
}
