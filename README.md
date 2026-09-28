# 3DEye Sorting Demo

This solution contains a small .NET sample for generating large text files and sorting them with an external merge sort strategy.

## Overview

The repository includes two projects:

- `Sorter` - reads a large input file, splits it into sorted chunks, and merges those chunks into a final sorted output file.
- `TestFileGenerator` - creates a large randomized test file that can be used to validate the sorting behavior at scale.

## Solution Structure

- `Sorter/Program.cs` - command-line entry point for sorting a file.
- `Sorter/FileSorter.cs` - orchestrates the chunking and merge workflow.
- `Sorter/FileChunkProcessor.cs` - handles file range splitting, parsing, chunk writing, and merge operations.
- `Sorter/LineEntry.cs` - represents a single sortable record in the format `number.text`.
- `Sorter/Options.cs` - command-line arguments for the sorter.
- `TestFileGenerator/Program.cs` - creates synthetic test data.
- `TestFileGenerator/Options.cs` - configuration for the generated file size and concurrency.

## How the sorter works

The sorting process follows a typical external merge sort approach:

1. The input file is divided into non-overlapping byte ranges aligned to line boundaries.
2. Multiple reader tasks read those ranges concurrently and push lines into a bounded channel.
3. Parser tasks read lines from the channel, accumulate them until a chunk limit is reached, and sort each chunk in memory.
4. Each sorted chunk is written to a temporary file in the system temp directory.
5. A k-way merge combines all temporary chunk files into one final sorted output.

This keeps memory usage bounded even when the source file is larger than available RAM.

## Running the sorter

From the solution root, run:

```bash
dotnet run --project Sorter -- --filePath "C:\path\to\input.txt" --chunkSize 100 --concurrencyCount 4
```

### Arguments

- `--filePath` - path to the input file to sort.
- `--chunkSize` - size of each temporary chunk in MB.
- `--concurrencyCount` - number of concurrent parsing and chunk-processing workers.

If `filePath` is omitted, the sorter defaults to a file named `test-file.txt` in the app output folder.

## Generating test files

You can generate a large synthetic file with:

```bash
dotnet run --project TestFileGenerator -- --fileSize 100 --producerCount 4 --batchSize 10000 --filePath "C:\temp\test-file.txt"
```

### Arguments

- `--fileSize` - target file size in MB.
- `--producerCount` - number of concurrent producer tasks.
- `--batchSize` - number of rows generated per batch.
- `--filePath` - destination file path.

## Notes

- The sorting logic assumes each line follows the pattern `number.text`.
- Invalid or malformed lines are skipped with a warning during chunk parsing.
- Temporary sorted chunk files are cleaned up automatically when the processor is disposed.

## Requirements

- .NET SDK 10.0 or newer.
- A Windows or Linux environment capable of running .NET console applications.
