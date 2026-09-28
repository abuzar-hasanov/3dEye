namespace Sorter;

/// <summary>
/// Orchestrates the external merge sort process
/// </summary>
internal class FileSorter
{
    private readonly int _chunkSizeMB;
    private readonly int _readerCount;

    /// <summary>
    /// Initializes the sorter with the desired chunk size and maximum parallelism.
    /// </summary>
    /// <param name="chunkSizeMB">The maximum size of each temporary chunk in megabytes.</param>
    /// <param name="readerCount">The maximum number of worker tasks used during chunking and merge preparation.</param>
    public FileSorter(int chunkSizeMB = 100, int readerCount = 0)
    {
        if (chunkSizeMB <= 0)
            throw new ArgumentException("Chunk size must be greater than 0", nameof(chunkSizeMB));
        if (readerCount < 0)
            throw new ArgumentException("Reader count cannot be negative", nameof(readerCount));

        _chunkSizeMB = chunkSizeMB;
        _readerCount = readerCount;
    }

    /// <summary>
    /// Splits the input file into sorted chunks, writes each chunk to a temporary file,
    /// and then merges those sorted chunk files into a single output file.
    /// </summary>
    /// <param name="filePath">The path to the input file that should be sorted.</param>
    /// <param name="cancellationToken">Token used to cancel the long-running sort operation.</param>
    public async Task SortAsync(string filePath, CancellationToken cancellationToken)
    {
        string fileNameOnly = Path.GetFileNameWithoutExtension(filePath);
        string extension = Path.GetExtension(filePath);
        var tempOutputPath = fileNameOnly + "-sorted" + extension;
        var processor = new FileChunkProcessor(_chunkSizeMB, _readerCount);
        try
        {

            Console.WriteLine("Reading and sorting file chunks...");
            var chunkFiles = await processor.SplitAndSortChunksAsync(filePath, cancellationToken);

            Console.WriteLine($"\nCreated {chunkFiles.Count} sorted chunk(s).");

            if (chunkFiles.Count > 0)
            {
                Console.WriteLine("Merging sorted chunks...");
                var tempOutput = tempOutputPath + ".tmp";
                processor.MergeChunks(chunkFiles, tempOutput, cancellationToken: cancellationToken);

                Console.WriteLine("\nRenaming output file...");
                if (File.Exists(tempOutputPath))
                {
                    File.Delete(tempOutputPath);
                }
                File.Move(tempOutput, tempOutputPath);
                Console.WriteLine($"Sorted file written to: {tempOutputPath}");
            }
        }
        catch
        {
            if (File.Exists(tempOutputPath))
                File.Delete(tempOutputPath);
            throw;
        }
        finally
        {
            processor.Dispose();
        }
    }
}
