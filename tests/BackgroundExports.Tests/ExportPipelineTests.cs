using System.IO.Compression;
using BackgroundExports.Core;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;

namespace BackgroundExports.Tests;

public sealed class ExportPipelineTests : IDisposable
{
    private readonly SqliteConnection _connection = new("DataSource=:memory:");
    private readonly string _output = Path.Combine(Path.GetTempPath(), "bgx-" + Guid.NewGuid().ToString("N"));
    private readonly TestFactory _factory;
    private readonly TestClock _clock = new();
    private readonly ExportOptions _options;
    private readonly BlockingGenerator _blocking = new();
    private readonly ExportQueue _queue;
    private readonly JobProcessor _processor;

    public ExportPipelineTests()
    {
        _connection.Open();
        _factory = new TestFactory(new DbContextOptionsBuilder<ExportsDbContext>().UseSqlite(_connection).Options);
        using (var db = _factory.CreateDbContext()) db.Database.EnsureCreated();
        Directory.CreateDirectory(_output);
        _options = new ExportOptions { OutputDirectory = _output, RetryBaseDelay = TimeSpan.FromSeconds(5) };

        IExportGenerator[] generators =
        [
            new InventoryExportGenerator(TimeSpan.Zero),
            new FlakyInventoryExportGenerator(TimeSpan.Zero),
            new AlwaysTransientGenerator(),
            new BrokenGenerator(),
            _blocking,
        ];
        var notifier = new JobNotifier();
        var cancellation = new JobCancellation();
        _queue = new ExportQueue(_factory, generators, _options, new JobSignal(), notifier, cancellation, _clock);
        _processor = new JobProcessor(_factory, generators, _options, notifier, cancellation, _clock, NullLogger<JobProcessor>.Instance);
    }

    public void Dispose()
    {
        _connection.Dispose();
        Directory.Delete(_output, recursive: true);
    }

    [Fact]
    public async Task Enqueue_only_records_the_request()
    {
        var job = await _queue.EnqueueAsync("ana", "Inventory", 10_000);

        Assert.Equal(JobStatus.Queued, (await GetAsync(job.Id)).Status);
        Assert.Empty(Directory.EnumerateFiles(_output));
    }

    [Fact]
    public async Task Worker_produces_a_zip_with_every_row()
    {
        var job = await _queue.EnqueueAsync("ana", "Inventory", 5_001);

        Assert.True(await _processor.RunNextAsync(CancellationToken.None));

        var done = await GetAsync(job.Id);
        Assert.Equal(JobStatus.Succeeded, done.Status);
        Assert.Equal(100, done.Progress);
        Assert.Equal(1, done.Attempts);
        var result = await _queue.OpenResultAsync(job.Id, "ana");
        Assert.NotNull(result);
        await using var stream = result.Value.Content;
        using var zip = new ZipArchive(stream);
        using var reader = new StreamReader(zip.GetEntry("inventory.csv")!.Open());
        var lines = (await reader.ReadToEndAsync()).Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(5_001 + 1, lines.Length); // header + rows
    }

    [Fact]
    public async Task Empty_queue_returns_false()
    {
        Assert.False(await _processor.RunNextAsync(CancellationToken.None));
    }

    [Fact]
    public async Task Oldest_job_runs_first()
    {
        var first = await _queue.EnqueueAsync("ana", "Inventory", 10);
        _clock.Advance(TimeSpan.FromSeconds(1));
        var second = await _queue.EnqueueAsync("ben", "Inventory", 10);

        await _processor.RunNextAsync(CancellationToken.None);

        Assert.Equal(JobStatus.Succeeded, (await GetAsync(first.Id)).Status);
        Assert.Equal(JobStatus.Queued, (await GetAsync(second.Id)).Status);
    }

    [Fact]
    public async Task Each_user_is_limited_to_three_active_exports()
    {
        for (var i = 0; i < 3; i++) await _queue.EnqueueAsync("ana", "Inventory", 10);

        await Assert.ThrowsAsync<ExportRequestException>(() => _queue.EnqueueAsync("ana", "Inventory", 10));
        await _queue.EnqueueAsync("ben", "Inventory", 10); // other users are unaffected
    }

    [Theory]
    [InlineData("Inventory", 0)]
    [InlineData("Inventory", 2_000_000)]
    [InlineData("Payroll", 10)]
    public async Task Invalid_requests_are_rejected(string type, int rows)
    {
        await Assert.ThrowsAsync<ExportRequestException>(() => _queue.EnqueueAsync("ana", type, rows));
    }

    [Fact]
    public async Task Only_the_requester_can_download()
    {
        var job = await _queue.EnqueueAsync("ana", "Inventory", 10);
        await _processor.RunNextAsync(CancellationToken.None);

        Assert.Null(await _queue.OpenResultAsync(job.Id, "ben"));
        var mine = await _queue.OpenResultAsync(job.Id, "ana");
        Assert.NotNull(mine);
        await mine.Value.Content.DisposeAsync();
    }

    [Fact]
    public async Task Transient_failure_waits_for_backoff_then_succeeds()
    {
        var job = await _queue.EnqueueAsync("ana", "Inventory (flaky source)", 10_000);

        await _processor.RunNextAsync(CancellationToken.None);
        var afterFirst = await GetAsync(job.Id);
        Assert.Equal(JobStatus.Queued, afterFirst.Status);
        Assert.Contains("timed out", afterFirst.Error);
        Assert.Empty(Directory.EnumerateFiles(_output)); // partial output cleaned up

        Assert.False(await _processor.RunNextAsync(CancellationToken.None)); // still backing off
        _clock.Advance(TimeSpan.FromSeconds(5));
        Assert.True(await _processor.RunNextAsync(CancellationToken.None));

        var done = await GetAsync(job.Id);
        Assert.Equal(JobStatus.Succeeded, done.Status);
        Assert.Equal(2, done.Attempts);
        Assert.Null(done.Error);
    }

    [Fact]
    public async Task Gives_up_after_max_attempts_with_doubling_delays()
    {
        var job = await _queue.EnqueueAsync("ana", "Always transient", 10);

        await _processor.RunNextAsync(CancellationToken.None);   // attempt 1, wait 5s
        _clock.Advance(TimeSpan.FromSeconds(5));
        await _processor.RunNextAsync(CancellationToken.None);   // attempt 2, wait 10s
        _clock.Advance(TimeSpan.FromSeconds(9));
        Assert.False(await _processor.RunNextAsync(CancellationToken.None));
        _clock.Advance(TimeSpan.FromSeconds(1));
        await _processor.RunNextAsync(CancellationToken.None);   // attempt 3, final

        var failed = await GetAsync(job.Id);
        Assert.Equal(JobStatus.Failed, failed.Status);
        Assert.Equal(3, failed.Attempts);
        Assert.Equal("Failed after 3 attempts: Source unavailable.", failed.Error);
    }

    [Fact]
    public async Task Unexpected_errors_fail_immediately_without_leaking_details()
    {
        var job = await _queue.EnqueueAsync("ana", "Broken", 10);

        await _processor.RunNextAsync(CancellationToken.None);

        var failed = await GetAsync(job.Id);
        Assert.Equal(JobStatus.Failed, failed.Status);
        Assert.Equal(1, failed.Attempts);
        Assert.DoesNotContain("password", failed.Error);
    }

    [Fact]
    public async Task Canceling_a_queued_job_means_it_never_runs()
    {
        var job = await _queue.EnqueueAsync("ana", "Inventory", 10);

        await _queue.CancelAsync(job.Id, "ana");

        Assert.False(await _processor.RunNextAsync(CancellationToken.None));
        Assert.Equal(JobStatus.Canceled, (await GetAsync(job.Id)).Status);
    }

    [Fact]
    public async Task Canceling_a_running_job_stops_it_and_leaves_no_file()
    {
        var job = await _queue.EnqueueAsync("ana", "Blocking", 10);
        var run = _processor.RunNextAsync(CancellationToken.None);
        await _blocking.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await _queue.CancelAsync(job.Id, "ana");
        await run.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(JobStatus.Canceled, (await GetAsync(job.Id)).Status);
        Assert.Empty(Directory.EnumerateFiles(_output));
    }

    [Fact]
    public async Task Other_users_cannot_cancel_your_export()
    {
        var job = await _queue.EnqueueAsync("ana", "Inventory", 10);
        await _queue.CancelAsync(job.Id, "ben");
        Assert.Equal(JobStatus.Queued, (await GetAsync(job.Id)).Status);
    }

    [Fact]
    public async Task Shutdown_mid_export_is_requeued_on_next_start_without_losing_an_attempt()
    {
        var job = await _queue.EnqueueAsync("ana", "Blocking", 10);
        using var stopping = new CancellationTokenSource();
        var run = _processor.RunNextAsync(stopping.Token);
        await _blocking.Started.Task.WaitAsync(TimeSpan.FromSeconds(5));

        stopping.Cancel();
        await run.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(JobStatus.Running, (await GetAsync(job.Id)).Status); // what a crash would leave

        Assert.Equal(1, await _processor.RecoverInterruptedAsync());
        var recovered = await GetAsync(job.Id);
        Assert.Equal(JobStatus.Queued, recovered.Status);
        Assert.Equal(0, recovered.Attempts);
    }

    [Fact]
    public async Task Two_workers_cannot_claim_the_same_job()
    {
        var job = await _queue.EnqueueAsync("ana", "Inventory", 10);
        await using var a = _factory.CreateDbContext();
        await using var b = _factory.CreateDbContext();
        var seenByA = await a.Jobs.SingleAsync(j => j.Id == job.Id);
        var seenByB = await b.Jobs.SingleAsync(j => j.Id == job.Id);

        seenByA.Status = JobStatus.Running; seenByA.ClaimToken = Guid.NewGuid();
        await a.SaveChangesAsync();
        seenByB.Status = JobStatus.Running; seenByB.ClaimToken = Guid.NewGuid();

        await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => b.SaveChangesAsync());
    }

    [Fact]
    public async Task Results_expire_and_their_files_are_deleted()
    {
        var job = await _queue.EnqueueAsync("ana", "Inventory", 10);
        await _processor.RunNextAsync(CancellationToken.None);

        _clock.Advance(TimeSpan.FromHours(23));
        Assert.Equal(0, await _processor.ExpireResultsAsync());
        _clock.Advance(TimeSpan.FromHours(2));
        Assert.Equal(1, await _processor.ExpireResultsAsync());

        Assert.Equal(JobStatus.Expired, (await GetAsync(job.Id)).Status);
        Assert.Empty(Directory.EnumerateFiles(_output));
        Assert.Null(await _queue.OpenResultAsync(job.Id, "ana"));
    }

    private async Task<ExportJob> GetAsync(Guid id)
    {
        await using var db = _factory.CreateDbContext();
        return await db.Jobs.AsNoTracking().SingleAsync(j => j.Id == id);
    }

    private sealed class TestFactory(DbContextOptions<ExportsDbContext> options) : IDbContextFactory<ExportsDbContext>
    {
        public ExportsDbContext CreateDbContext() => new(options);
    }

    private sealed class TestClock : TimeProvider
    {
        private DateTimeOffset _now = new(2026, 3, 2, 9, 0, 0, TimeSpan.Zero);
        public override DateTimeOffset GetUtcNow() => _now;
        public void Advance(TimeSpan by) => _now += by;
    }

    private sealed class AlwaysTransientGenerator : IExportGenerator
    {
        public string ExportType => "Always transient";
        public Task<string> GenerateAsync(ExportJob job, Stream output, IProgress<int> progress, CancellationToken ct) =>
            throw new TransientExportException("Source unavailable.");
    }

    private sealed class BrokenGenerator : IExportGenerator
    {
        public string ExportType => "Broken";
        public Task<string> GenerateAsync(ExportJob job, Stream output, IProgress<int> progress, CancellationToken ct) =>
            throw new InvalidOperationException("Login failed for user 'sa', password=hunter2");
    }

    private sealed class BlockingGenerator : IExportGenerator
    {
        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public string ExportType => "Blocking";

        public async Task<string> GenerateAsync(ExportJob job, Stream output, IProgress<int> progress, CancellationToken ct)
        {
            await output.WriteAsync(new byte[100], ct); // partial output that must be cleaned up
            Started.TrySetResult();
            await Task.Delay(Timeout.Infinite, ct);
            return "never.zip";
        }
    }
}
