using CommandLine;

internal partial class Program
{
    public class Options
    {
        [Option('s', "fileSize", Default = 1, HelpText = "Size of the file to generate (in MB). Default is 1 MB.")]
        public int FileSize { get; set; }

        [Option('p', "filePath", Default = null, HelpText = "Path where the generated file will be saved. Defaults to 'test-file.txt' in the application output folder.")]
        public string FilePath { get; set; }

        [Option('t', "producerCount", Default = 0, HelpText = "Number of producer tasks. Defaults to the processor count.")]
        public int ProducerCount { get; set; }

        [Option('t', "batchSize", Default = 10000, HelpText = "Number of lines to be generated per batch. Defaults is 10000.")]
        public int BatchSize { get; set; }

    }
}