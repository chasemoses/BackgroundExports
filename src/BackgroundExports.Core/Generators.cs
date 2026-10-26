using System.Globalization;
using System.IO.Compression;
using System.Text;

namespace BackgroundExports.Core;

public interface IExportGenerator
{
    string ExportType { get; }

    /// <summary>Writes the export to <paramref name="output"/> and returns the download file name.</summary>
    Task<string> GenerateAsync(ExportJob job, Stream output, IProgress<int> progress, CancellationToken ct);
}

/// <summary>
/// An inventory export: a ZIP holding one CSV. Rows are produced in chunks with a configurable
/// pause, standing in for the slow queries a real export would run against a source system.
/// </summary>
public class InventoryExportGenerator(TimeSpan delayPerChunk) : IExportGenerator
{
    private static readonly string[] Buildings = ["Austin", "Hart", "Kimball", "Manwaring", "Romney", "Smith", "Snow", "Spori", "Taylor"];
    private static readonly string[] Categories = ["Projector", "Display", "Camera", "Printer", "Workstation", "Air handler", "Door controller"];
    protected const int ChunkSize = 2_000;

    public virtual string ExportType => "Inventory";

    public async Task<string> GenerateAsync(ExportJob job, Stream output, IProgress<int> progress, CancellationToken ct)
    {
        using (var zip = new ZipArchive(output, ZipArchiveMode.Create, leaveOpen: true))
        {
            var entry = zip.CreateEntry("inventory.csv", CompressionLevel.Fastest);
            await using var stream = entry.Open();
            await using var writer = new StreamWriter(stream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: true));
            await writer.WriteLineAsync("tag,category,building,room,acquired,cost");

            var rng = new Random(job.Id.GetHashCode());
            for (var written = 0; written < job.RowCount;)
            {
                ct.ThrowIfCancellationRequested();
                await BeforeChunkAsync(job, written, ct);
                var end = Math.Min(job.RowCount, written + ChunkSize);
                for (; written < end; written++)
                {
                    var line = string.Create(CultureInfo.InvariantCulture,
                        $"EQ-{written + 1:D7},{Categories[rng.Next(Categories.Length)]},{Buildings[rng.Next(Buildings.Length)]},{rng.Next(100, 399)},{new DateOnly(2012, 1, 1).AddDays(rng.Next(5000)):yyyy-MM-dd},{rng.Next(150, 25_000)}.00");
                    await writer.WriteLineAsync(line);
                }
                progress.Report(written * 100 / job.RowCount);
                if (delayPerChunk > TimeSpan.Zero) await Task.Delay(delayPerChunk, ct);
            }
        }
        return $"{ExportType.ToLowerInvariant().Replace(' ', '-')}-{job.CreatedAt:yyyyMMdd-HHmm}.zip";
    }

    protected virtual Task BeforeChunkAsync(ExportJob job, int rowsWritten, CancellationToken ct) => Task.CompletedTask;
}

/// <summary>
/// Same export from a source that times out partway through on the first attempt.
/// Exists so the retry path can be seen working in the demo.
/// </summary>
public sealed class FlakyInventoryExportGenerator(TimeSpan delayPerChunk) : InventoryExportGenerator(delayPerChunk)
{
    public override string ExportType => "Inventory (flaky source)";

    protected override Task BeforeChunkAsync(ExportJob job, int rowsWritten, CancellationToken ct) =>
        job.Attempts == 1 && rowsWritten >= job.RowCount * 4 / 10
            ? throw new TransientExportException("Source system timed out.")
            : Task.CompletedTask;
}
