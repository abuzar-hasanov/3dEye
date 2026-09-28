using System.Diagnostics;
using System.Text;
using System.Threading.Channels;
using CommandLine;

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
                    var producerCount = options.ProducerCount > 0 ? options.ProducerCount : Environment.ProcessorCount;
                    Console.WriteLine("Generating test file with size {0} MB, {1} producers and batch size {2}:", options.FileSize, producerCount, options.BatchSize);
                    await GenerateTestFile(filePath, options.FileSize, producerCount, options.BatchSize, cancellationTokenSource.Token);
                    Console.WriteLine("File generated successfully at: {0}", filePath);
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("File generation cancelled.");
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine("Error generating file: {0}", ex.Message);
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

    /// <summary>
    /// Generates a large, randomized test file using multiple producer tasks and a bounded channel queue.
    /// </summary>
    /// <param name="filePath">The output path for the generated file.</param>
    /// <param name="fileSizeMB">The target size of the generated file in megabytes.</param>
    /// <param name="producerCount">The number of worker tasks that produce data concurrently.</param>
    /// <param name="batchSize">The number of lines each producer writes per batch.</param>
    /// <param name="cancellationToken">The token used to stop file generation.</param>
    static async Task GenerateTestFile(string filePath, int fileSizeMB, int producerCount, int batchSize,
        CancellationToken cancellationToken)
    {
        var stopWatch = Stopwatch.StartNew();
        var maxQueueSize = 50; // Max batches in queue
        var targetBytes = (long)fileSizeMB * 1024 * 1024;

        // Thread-safe queue for producer-consumer pattern
        var batchQueue = Channel.CreateBounded<StringBuilder>(new BoundedChannelOptions(maxQueueSize)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });

        var consumerTask = GetConsumerTask(filePath, batchQueue, cancellationToken);

        try
        {
            // Produce batches in parallel
            var producerTasks = new Task[producerCount];
            long bytesGeneratedTotal = 0;
            var maxValueForRandom = fileSizeMB * 1000;
            var maxLineLength = maxValueForRandom.ToString().Length + 1 + 32 + Environment.NewLine.Length;
            var batchCapacity = batchSize * maxLineLength;

            var sampleTexts = Enumerable.Range(0, fileSizeMB * 100)
                                   .Select(_ => Guid.NewGuid().ToString("N"))
                                   .ToArray();
            var randomSeed = Random.Shared.Next();

            for (int i = 0; i < producerCount; i++)
            {
                producerTasks[i] = Task.Run(async () =>
                {
                    var random = new Random(Interlocked.Increment(ref randomSeed));

                    while (true)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        if (Volatile.Read(ref bytesGeneratedTotal) >= targetBytes)
                            break;

                        var sb = new StringBuilder(batchCapacity);

                        for (int j = 0; j < batchSize; j++)
                        {
                            sb.Append(random.Next(1, maxValueForRandom))
                                .Append('.')
                                .AppendLine(sampleTexts[random.Next(sampleTexts.Length)]);
                        }

                        Interlocked.Add(ref bytesGeneratedTotal, sb.Length);
                        await batchQueue.Writer.WriteAsync(sb, cancellationToken);
                    }
                }, cancellationToken);
            }

            await Task.WhenAll(producerTasks);
            batchQueue.Writer.TryComplete();

            await consumerTask;
        }
        finally
        {
            batchQueue.Writer.TryComplete();
        }

        stopWatch.Stop();
        Console.WriteLine($"File generation is done in {stopWatch.ElapsedMilliseconds} ms");
    }

    /// <summary>
    /// Consumes generated batches from the queue and writes them to disk as a single ordered text file.
    /// </summary>
    /// <param name="filePath">The destination file path.</param>
    /// <param name="batchQueue">The producer-consumer queue containing generated StringBuilder batches.</param>
    /// <param name="cancellationToken">The token used to stop output writing.</param>
    static async Task GetConsumerTask(string filePath, Channel<StringBuilder> batchQueue, CancellationToken cancellationToken)
    {
        // Start writer task
        var bufferSize = 1024 * 1024; // 1 MB buffer for StreamWriter
        using var fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize, FileOptions.SequentialScan);
        using var writer = new StreamWriter(fileStream, Encoding.UTF8, bufferSize);
        await foreach (var batch in batchQueue.Reader.ReadAllAsync(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            writer.Write(batch);
        }
    }
}