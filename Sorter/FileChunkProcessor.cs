using System.Threading.Channels;

namespace Sorter;

/// <summary>
/// Handles reading, sorting, and writing file chunks for external merge sort
/// </summary>
internal class FileChunkProcessor : IDisposable
{
    private readonly long _chunkSizeBytes;
    private readonly int _concurrencyCount;
    //private readonly LineEntryComparer _comparer = new();
    private readonly string _tempDirectory;
    private bool _disposed;
    private readonly int _bufferSize = 65536; // 64KB buffer for reading/writing    

    /// <summary>
    /// Creates a new FileChunkProcessor with specified chunk size
    /// </summary>
    /// <param name="chunkSizeMB">Size of each chunk in megabytes (default 100MB)</param>
    public FileChunkProcessor(int chunkSizeMB = 100, int concurrencyCount = 0)
    {
        if (chunkSizeMB <= 0)
            throw new ArgumentException("Chunk size must be greater than 0", nameof(chunkSizeMB));
        if (concurrencyCount < 0)
            throw new ArgumentException("Reader count cannot be negative", nameof(concurrencyCount));

        _chunkSizeBytes = (long)chunkSizeMB * 1024 * 1024;
        _concurrencyCount = concurrencyCount > 0 ? concurrencyCount : Math.Min(Environment.ProcessorCount, 4);
        _tempDirectory = Path.Combine(Path.GetTempPath(), $"sorter_{Guid.NewGuid()}");
        Directory.CreateDirectory(_tempDirectory);
    }

    /// <summary>
    /// Reads the input file in parallel, parses each line into a sortable record,
    /// batches records into memory-limited chunks, and writes each chunk to a temporary sorted file.
    /// </summary>
    /// <param name="inputFilePath">The path to the source file to chunk and sort.</param>
    /// <param name="cancellationToken">The token used to stop work if the caller cancels.</param>
    /// <returns>A list of temporary chunk files containing sorted entries in chunk order.</returns>
    public async Task<List<string>> SplitAndSortChunksAsync(string inputFilePath, CancellationToken cancellationToken)
    {
        var chunkFiles = new List<string>();
        var invalidLines = 0;

        var lineChannel = Channel.CreateBounded<string>(
            new BoundedChannelOptions(_concurrencyCount)
            {
                SingleReader = false,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait,
                AllowSynchronousContinuations = false
            });
        var processingTasks = new System.Collections.Concurrent.ConcurrentBag<Task>();
        var parserTasks = new List<Task>();
        using var processingSlots = new SemaphoreSlim(_concurrencyCount);
        var chunkPaths = new System.Collections.Concurrent.ConcurrentDictionary<int, string>();
        var nextChunkNumber = 0;
        var readerTasks = new List<Task>();

        try
        {
            var fileByteRanges = await DivideFileIntoRangesAsync(inputFilePath, _concurrencyCount, cancellationToken);
            foreach (var range in fileByteRanges)
            {
                readerTasks.Add(ReadFileRangeIntoChannelAsync(inputFilePath, range, lineChannel.Writer, cancellationToken));
            }

            var channelCompletionTask = CompleteChannelWhenReadersFinishAsync(readerTasks, lineChannel.Writer);

            var initialEntriesCapacity = _bufferSize;
            for (var i = 0; i < _concurrencyCount; i++)
            {
                parserTasks.Add(Task.Run(async () =>
                {
                    var entries = new List<LineEntry>(initialEntriesCapacity);
                    long currentChunkSize = 0;

                    await foreach (var line in lineChannel.Reader.ReadAllAsync(cancellationToken))
                    {
                        if (LineEntry.TryParse(line, out var entry))
                        {
                            entries.Add(entry);
                            currentChunkSize += line.Length + 2;
                        }
                        else
                        {
                            Interlocked.Increment(ref invalidLines);
                        }

                        if (currentChunkSize >= _chunkSizeBytes)
                        {
                            await QueueChunkForProcessingAsync(entries, Interlocked.Increment(ref nextChunkNumber) - 1);
                            entries = new List<LineEntry>(initialEntriesCapacity);
                            currentChunkSize = 0;
                        }
                    }

                    if (entries.Count > 0)
                    {
                        await QueueChunkForProcessingAsync(entries, Interlocked.Increment(ref nextChunkNumber) - 1);
                    }
                }, cancellationToken));
            }

            await Task.WhenAll(parserTasks);
            await channelCompletionTask;
            await Task.WhenAll(processingTasks);

            if (invalidLines > 0)
            {
                Console.WriteLine($"Warning: {invalidLines} invalid lines skipped during parsing.");
            }

            var orderedChunkPaths = chunkPaths.OrderBy(pair => pair.Key).Select(pair => pair.Value).ToList();
            foreach (var chunkPath in orderedChunkPaths)
            {
                if (File.Exists(chunkPath))
                {
                    chunkFiles.Add(chunkPath);
                }
            }

        }
        catch (IOException ioEx)
        {
            throw new InvalidOperationException($"Error reading file: {ioEx.Message}", ioEx);
        }

        return chunkFiles;

        async Task QueueChunkForProcessingAsync(List<LineEntry> entries, int chunkNumber)
        {
            await processingSlots.WaitAsync(cancellationToken);
            processingTasks.Add(Task.Run(() =>
            {
                try
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    chunkPaths[chunkNumber] = WriteSortedChunk(entries, chunkNumber);
                }
                finally
                {
                    processingSlots.Release();
                }
            }));
        }
    }

    /// <summary>
    /// Divides the file into non-overlapping byte ranges that are aligned to line boundaries
    /// so multiple reader tasks can process the same file without splitting mid-record.
    /// </summary>
    /// <param name="filePath">The path to the source file.</param>
    /// <param name="rangeCount">The number of logical reading ranges to create.</param>
    /// <param name="cancellationToken">The cancellation token for this stage.</param>
    /// <returns>A list of inclusive/exclusive byte ranges.</returns>
    private async Task<List<(long start, long end)>> DivideFileIntoRangesAsync(
        string filePath, int rangeCount, CancellationToken cancellationToken)
    {
        var fileSize = new FileInfo(filePath).Length;
        var ranges = new List<(long start, long end)>();
        if (fileSize == 0)
        {
            return ranges;
        }

        var boundaries = new List<long> { 0 };
        await using (var fileStream = new FileStream(filePath, new FileStreamOptions
        {
            Mode = FileMode.Open,
            Access = FileAccess.Read,
            Share = FileShare.Read,
            BufferSize = _bufferSize,
            Options = FileOptions.Asynchronous | FileOptions.RandomAccess
        }))
        {
            var buffer = new byte[_bufferSize];
            for (var i = 1; i < rangeCount; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var target = fileSize / rangeCount * i + (fileSize % rangeCount * i) / rangeCount;
                var boundary = await FindNextLineBoundaryAsync(
                    fileStream, target, fileSize, buffer, cancellationToken);
                if (boundary > boundaries[^1])
                {
                    boundaries.Add(boundary);
                }
            }
        }

        boundaries.Add(fileSize);
        for (var i = 1; i < boundaries.Count; i++)
        {
            if (boundaries[i] > boundaries[i - 1])
            {
                ranges.Add((boundaries[i - 1], boundaries[i]));
            }
        }

        return ranges;
    }

    /// <summary>
    /// Scans forward from a given offset until the next newline is found so a reader range starts cleanly.
    /// </summary>
    /// <param name="fileStream">The file stream being scanned.</param>
    /// <param name="start">The offset where the scan should begin.</param>
    /// <param name="fileSize">The total file length.</param>
    /// <param name="buffer">A reusable buffer used during the read loop.</param>
    /// <param name="cancellationToken">The cancellation token for the scan.</param>
    /// <returns>The offset immediately after the next newline character, or the end-of-file offset if none exists.</returns>
    private static async Task<long> FindNextLineBoundaryAsync(
        FileStream fileStream, long start, long fileSize, byte[] buffer, CancellationToken cancellationToken)
    {
        fileStream.Seek(start, SeekOrigin.Begin);
        var position = start;
        while (position < fileSize)
        {
            var bytesToRead = (int)Math.Min(buffer.Length, fileSize - position);
            var bytesRead = await fileStream.ReadAsync(buffer.AsMemory(0, bytesToRead), cancellationToken);
            if (bytesRead == 0)
            {
                break;
            }

            var newlineIndex = Array.IndexOf(buffer, (byte)'\n', 0, bytesRead);
            if (newlineIndex >= 0)
            {
                return position + newlineIndex + 1;
            }

            position += bytesRead;
        }

        return fileSize;
    }

    /// <summary>
    /// Reads one file segment line-by-line and pushes each record into the shared processing queue.
    /// </summary>
    /// <param name="filePath">The source file path.</param>
    /// <param name="range">The byte range assigned to this reader.</param>
    /// <param name="lineWriter">The channel that receives parsed lines.</param>
    /// <param name="cancellationToken">Cancellation token for the read loop.</param>
    private async Task ReadFileRangeIntoChannelAsync(string filePath, (long start, long end) range,
        ChannelWriter<string> lineWriter, CancellationToken cancellationToken)
    {
        try
        {
            await using var fileStream = new FileStream(filePath, new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.Read,
                BufferSize = _bufferSize,
                Options = FileOptions.Asynchronous | FileOptions.SequentialScan
            });
            fileStream.Seek(range.start, SeekOrigin.Begin);
            using var boundedStream = new LimitedReadStream(fileStream, range.end - range.start);
            using var reader = new StreamReader(boundedStream, System.Text.Encoding.UTF8, false, _bufferSize);
            string? line;
            while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
            {
                await lineWriter.WriteAsync(line, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error reading file range {range.start}-{range.end}: {ex.Message}");
            throw;
        }
    }

    private sealed class LimitedReadStream : Stream
    {
        private readonly Stream _innerStream;
        private readonly long _length;
        private long _bytesRead;

        public LimitedReadStream(Stream innerStream, long length)
        {
            _innerStream = innerStream;
            _length = length;
        }

        public override bool CanRead => _innerStream.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => _length;
        public override long Position
        {
            get => _bytesRead;
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var bytesRead = _innerStream.Read(buffer, offset, GetReadLength(count));
            _bytesRead += bytesRead;
            return bytesRead;
        }

        public override int Read(Span<byte> buffer)
        {
            var bytesRead = _innerStream.Read(buffer[..GetReadLength(buffer.Length)]);
            _bytesRead += bytesRead;
            return bytesRead;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            var bytesRead = await _innerStream.ReadAsync(buffer[..GetReadLength(buffer.Length)], cancellationToken);
            _bytesRead += bytesRead;
            return bytesRead;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count,
            CancellationToken cancellationToken) => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        private int GetReadLength(int requestedLength) =>
            (int)Math.Min(requestedLength, _length - _bytesRead);
    }

    /// <summary>
    /// Waits for all reader tasks to finish and closes the channel so parser tasks can complete cleanly.
    /// </summary>
    /// <param name="readerTasks">The tasks responsible for reading file ranges.</param>
    /// <param name="lineWriter">The channel writer that should be completed when no more data remains.</param>
    private static async Task CompleteChannelWhenReadersFinishAsync(
        List<Task> readerTasks,
        ChannelWriter<string> lineWriter)
    {
        try
        {
            await Task.WhenAll(readerTasks);
            lineWriter.TryComplete();
        }
        catch (Exception ex)
        {
            lineWriter.TryComplete(ex);
            throw;
        }
    }

    /// <summary>
    /// Sorts the entries in the current in-memory batch and writes them to a temporary chunk file.
    /// </summary>
    /// <param name="entries">The entries collected for a single chunk.</param>
    /// <param name="chunkNumber">The zero-based number used in the temporary output filename.</param>
    /// <returns>The full path of the generated chunk file.</returns>
    private string WriteSortedChunk(List<LineEntry> entries, int chunkNumber)
    {
        entries.Sort();

        var chunkPath = Path.Combine(_tempDirectory, $"chunk_{chunkNumber:D6}.tmp");
        using (var writer = new StreamWriter(chunkPath, false, System.Text.Encoding.UTF8, _bufferSize))
        {
            foreach (var entry in entries)
            {
                writer.Write(entry.Number);
                writer.Write('.');
                writer.WriteLine(entry.Text);
            }
        }

        return chunkPath;
    }

    /// <summary>
    /// Performs a k-way merge across the sorted temporary chunk files and writes the fully ordered output.
    /// </summary>
    /// <param name="chunkFiles">The temporary sorted chunk files to merge.</param>
    /// <param name="outputFilePath">The final destination for the merged output.</param>
    /// <param name="cancellationToken">Token used to stop the merge operation.</param>
    public void MergeChunks(List<string> chunkFiles, string outputFilePath, CancellationToken cancellationToken = default)
    {
        if (chunkFiles.Count == 0)
            return;

        if (chunkFiles.Count == 1)
        {
            File.Copy(chunkFiles[0], outputFilePath, overwrite: true);
            return;
        }

        // Use a priority queue (min-heap) to merge k sorted files
        var readers = new List<StreamReader>();
        var entries = new PriorityQueue<(LineEntry entry, int readerIndex), LineEntry>();

        try
        {
            // Open all readers and add first entry from each
            for (int i = 0; i < chunkFiles.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var reader = new StreamReader(chunkFiles[i], System.Text.Encoding.UTF8, false, _bufferSize);
                readers.Add(reader);

                if (TryReadNextEntry(reader, out var entry))
                {
                    entries.Enqueue((entry, i), entry);
                }
            }

            // Merge process
            using var writer = new StreamWriter(outputFilePath, false, System.Text.Encoding.UTF8, _bufferSize);
            while (entries.Count > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var (entry, readerIndex) = entries.Dequeue();
                writer.WriteLine(entry.ToString());

                // Try to read next entry from the same reader
                if (TryReadNextEntry(readers[readerIndex], out var nextEntry))
                {
                    entries.Enqueue((nextEntry, readerIndex), nextEntry);
                }
            }
        }
        finally
        {
            // Clean up readers
            foreach (var reader in readers)
            {
                reader?.Dispose();
            }
        }
    }

    /// <summary>
    /// Reads the next parsed record from a chunk file and returns it to the merge heap.
    /// </summary>
    /// <param name="reader">The stream reader for the active chunk file.</param>
    /// <param name="entry">The next parsed record, or null when the file is exhausted.</param>
    /// <returns>True when a valid next entry exists; otherwise false.</returns>
    private bool TryReadNextEntry(StreamReader reader, out LineEntry? entry)
    {
        var line = reader.ReadLine();
        if (line == null)
        {
            entry = null;
            return false;
        }

        return LineEntry.TryParse(line, out entry);
    }

    /// <summary>
    /// Cleans up temporary files
    /// </summary>
    public void Cleanup()
    {
        try
        {
            if (Directory.Exists(_tempDirectory))
            {
                Directory.Delete(_tempDirectory, recursive: true);
            }
        }
        catch
        {
            // Ignore cleanup errors
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        Cleanup();
        _disposed = true;
        GC.SuppressFinalize(this);
    }

    ~FileChunkProcessor()
    {
        Cleanup();
    }
}
