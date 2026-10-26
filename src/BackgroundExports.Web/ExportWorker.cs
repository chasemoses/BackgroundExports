using BackgroundExports.Core;

namespace BackgroundExports.Web;

/// <summary>
/// Hosts the processor: a fixed number of worker loops plus an hourly cleanup.
/// The loops sleep until a job is queued (or 5 seconds pass, to pick up retries and jobs
/// queued by other servers), so an idle app costs almost nothing.
/// </summary>
public sealed class ExportWorker(JobProcessor processor, JobSignal signal, IConfiguration config, ILogger<ExportWorker> log) : BackgroundService
{
    private readonly int _workers = config.GetValue("Exports:Workers", 2);

    protected override async Task ExecuteAsync(CancellationToken stopping)
    {
        var recovered = await processor.RecoverInterruptedAsync(stopping);
        if (recovered > 0) log.LogInformation("Requeued {Count} exports interrupted by the last shutdown", recovered);

        var loops = Enumerable.Range(0, _workers).Select(_ => WorkLoopAsync(stopping)).Append(CleanupLoopAsync(stopping));
        await Task.WhenAll(loops);
    }

    private async Task WorkLoopAsync(CancellationToken stopping)
    {
        while (!stopping.IsCancellationRequested)
        {
            try
            {
                if (!await processor.RunNextAsync(stopping))
                    await signal.WaitAsync(TimeSpan.FromSeconds(5), stopping);
            }
            catch (OperationCanceledException) when (stopping.IsCancellationRequested) { }
            catch (Exception ex)
            {
                // e.g. the database is briefly unreachable. Don't let one bad moment kill the worker.
                log.LogError(ex, "Export worker loop failed; pausing before trying again");
                await Task.Delay(TimeSpan.FromSeconds(10), stopping).ContinueWith(_ => { });
            }
        }
    }

    private async Task CleanupLoopAsync(CancellationToken stopping)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromHours(1));
        do
        {
            try { await processor.ExpireResultsAsync(stopping); }
            catch (Exception ex) when (!stopping.IsCancellationRequested) { log.LogError(ex, "Export cleanup failed"); }
        } while (await timer.WaitForNextTickAsync(stopping).AsTask().ContinueWith(t => !t.IsCanceled && t.Result));
    }
}
