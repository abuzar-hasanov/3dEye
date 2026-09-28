namespace Sorter;

/// <summary>
/// Represents a single line entry in the format "Number.Text"
/// </summary>
internal class LineEntry : IComparable<LineEntry>
{
    public int Number { get; set; }
    public string Text { get; set; }

    public LineEntry(int number, string text)
    {
        Number = number;
        Text = text;
    }

    /// <summary>
    /// Parses a line in format "Number.Text" into a LineEntry
    /// </summary>
    /// <param name="line">The line to parse</param>
    /// <returns>Parsed LineEntry or null if parsing fails</returns>
    public static bool TryParse(string? line, out LineEntry? entry)
    {
        entry = null;

        if (string.IsNullOrWhiteSpace(line))
            return false;

        var dotIndex = line.IndexOf('.');
        if (dotIndex <= 0 || dotIndex >= line.Length - 1)
            return false;

        if (!int.TryParse(line.AsSpan(0, dotIndex), out var number))
            return false;

        entry = new LineEntry(number, line.Substring(dotIndex + 1));
        return true;
    }

    public static LineEntry? TryParse(string? line)
    {
        return TryParse(line, out var entry) ? entry : null;
    }

    /// <summary>
    /// Returns the entry in the original input format, including the numeric prefix and text payload.
    /// </summary>
    public override string ToString()
    {
        return $"{Number}.{Text}";
    }

    /// <summary>
    /// Compares two entries by their text content and then by their numeric prefix as a tiebreaker.
    /// </summary>
    /// <param name="other">The other entry to compare against.</param>
    /// <returns>A value less than zero, zero, or greater than zero depending on the ordering.</returns>
    public int CompareTo(LineEntry? other)
    {
        if (other is null)
            return 1;

        int textComparisonResult = string.CompareOrdinal(Text, other.Text);
        return textComparisonResult != 0 ? textComparisonResult : Number.CompareTo(other.Number);
    }
}
