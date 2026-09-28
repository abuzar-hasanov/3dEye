using System.Diagnostics;
using CommandLine;
using Sorter;

internal partial class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("App is starting...");
        using var cancellationTokenSource = new CancellationTokenSource();
        Console.CancelKeyPress += (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            cancellationTokenSource.Cancel();
        };

        var parseResult = Parser.Default.ParseArguments<Options>(args);
        await parseResult
            .WithParsedAsync(async options =>
            {
                try
                {
                    string filePath = string.IsNullOrEmpty(options.FilePath)
                        ? Path.Combine(AppContext.BaseDirectory, $"test-file.txt")
                        : options.FilePath;
                    if (!File.Exists(filePath))
                    {
                        Console.WriteLine("File doesn't exist at: {0}.", filePath);
                        return;
                    }
                    Console.WriteLine("Starting to sort file at: {0}.", filePath);

                    var stopWatch = Stopwatch.StartNew();
                    var processorCount = options.ConcurrencyCount <= 0 ? Environment.ProcessorCount : options.ConcurrencyCount;
                    var sorter = new FileSorter(options.ChunkSizeMB, processorCount);
                    await sorter.SortAsync(filePath, cancellationTokenSource.Token);
                    stopWatch.Stop();
                    var fileSize = new FileInfo(filePath).Length / 1024 / 1024;
                    Console.WriteLine($"Sorting completed in {stopWatch.ElapsedMilliseconds} ms for {filePath} ({fileSize} MB).");
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Sorting cancelled.");
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine("Error sorting file: {0}", ex.Message);
                    Console.Error.WriteLine(ex.StackTrace);
                }
            });

        parseResult.WithNotParsed(errors =>
            {
                foreach (var error in errors)
                {
                    Console.Error.WriteLine("Error: {0}", error);
                }
            });
    }
}