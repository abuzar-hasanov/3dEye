using CommandLine;

internal partial class Program
{
    public class Options
    {
        [Option('p', "filePath", HelpText = "Path where the generated file is stored. Defaults to 'test-file.txt' in the application output folder.")]
        public string FilePath { get; set; }

        [Option('c', "chunkSize", Default = 1, HelpText = "Size of each memory chunk in MB (default: 1MB). Larger chunks are faster but use more memory.")]
        public int ChunkSizeMB { get; set; }

        [Option('r', "concurrencyCount", Default = 0, HelpText = "Number of concurrent operations. Defaults to environment processor count.")]
        public int ConcurrencyCount { get; set; }
    }
}
