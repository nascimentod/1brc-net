using BenchmarkDotNet.Attributes;
using System.Text;
using System.IO.MemoryMappedFiles;
using System.Collections.Concurrent;
using BenchmarkDotNet.Running;

internal class Program
{
    private static void Main(string[] args)
    {
        //BenchmarkRunner.Run<MeasurementCalculator>();
        new MeasurementCalculatorNew().CalculateMeasurements();
    }
}

[MemoryDiagnoser]
public class MeasurementCalculator
{

    [Benchmark]
    public void Old()
    {
        var old = new MeasurementCalculatorOld();
        old.CalculateMeasurements();
    }

    [Benchmark]
    public void New()
    {
        var nw = new MeasurementCalculatorNew();
        nw.CalculateMeasurements();
    }
}

public class MeasurementCalculatorOld
{
    [Benchmark]
    public void CalculateMeasurements()
    {
        var fileName = "/Users/davidnascimento/source/1brc-net/measurements_1b.txt";
        var fileSize = new FileInfo(fileName).Length;
        var dop = 20;

        var dictionary = new ConcurrentDictionary<string, Measurement>();

        using var mmf = MemoryMappedFile.CreateFromFile(fileName);
        ProcessFile(mmf, fileSize, dop, dictionary);

        //PrintToConsole(dictionary);
    }

    private void ProcessFile(MemoryMappedFile mmf, long fileSize, int degreeOfParallelisation, ConcurrentDictionary<string, Measurement> dictionary)
    {
        var chunkSize = fileSize / degreeOfParallelisation;
        var tasks = new Task[degreeOfParallelisation];

        var chunkBoundaries = new long[degreeOfParallelisation + 1]; // Calculate the boundaries of the chunks here
        chunkBoundaries[0] = 0L; // First chunk always starts at zero
        chunkBoundaries[degreeOfParallelisation] = fileSize; // Last chunk always ends at last byte of the file

        using (var accessor = mmf.CreateViewAccessor(0L, fileSize, MemoryMappedFileAccess.Read))
        {
            for (var i = 1; i < degreeOfParallelisation; i++)
            {
                var start = i * chunkSize;
                var offset = -1;
                byte character;

                do
                {
                    offset++;
                    accessor.Read(start + offset, out character);
                } while (character != '\n'); // While we don't hit a new line character, we keep moving forward

                chunkBoundaries[i] = start + offset;
            }
        }

        for (var i = 0; i < degreeOfParallelisation; i++)
        {
            var start = chunkBoundaries[i];
            var end = chunkBoundaries[i + 1];
            tasks[i] = Task.Run(() => ProcessChunk(mmf, start, end, dictionary));
        }

        Task.WaitAll(tasks);
    }

    private void ProcessChunk(MemoryMappedFile mmf, long start, long end, ConcurrentDictionary<string, Measurement> dictionary)
    {
        using var stream = mmf.CreateViewStream(start, end - start);
        using var reader = new BinaryReader(stream);
        var sb = new StringBuilder();

        try
        {
            while (true)
            {
                var character = reader.ReadChar();

                if (character == '\n')
                {
                    ProcessLine(dictionary, sb.ToString());
                    sb.Clear();
                }
                else
                {
                    sb.Append(character);
                }
            }
        }
        catch (EndOfStreamException)
        {
            // END
        }
    }

    private static void ProcessLine(ConcurrentDictionary<string, Measurement> dictionary, string line)
    {
        if (string.IsNullOrWhiteSpace(line))
        {
            return;
        }

        try
        {
            // Get city name
            int i = 0;
            while (i < line.Length && line[i] != ';')
            {
                i++;
            }

            var lineSpan = line.AsSpan();
            var city = lineSpan[..i];
            var measurement = lineSpan[(i + 1)..];

            i = 0;
            // Get measurement integer and decimal parts
            while (i < measurement.Length && measurement[i] != '.')
            {
                i++;
            }
            var integerPart = measurement[..i];
            var decimalPart = measurement[(i + 1)..];

            // Store everything as an int, we'll do the proper conversion to float during print
            var value = int.Parse(integerPart) * 10 + int.Parse(decimalPart);

            dictionary.AddOrUpdate(city.ToString(), new Measurement() { Min = value, Sum = value, Max = value, Count = 1 }, (_, measurement) =>
            {
                if (measurement.Min > value)
                {
                    measurement.Min = value;
                }
                if (measurement.Max < value)
                {
                    measurement.Max = value;
                }
                measurement.Sum += value;
                measurement.Count += 1uL;

                return measurement;
            });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"{line} was the problem");
        }
    }

    private static void PrintToConsole(ConcurrentDictionary<string, Measurement> dictionary)
    {
        var orderedCities = dictionary.OrderBy(x => x.Key).ToDictionary();

        var sb = new StringBuilder();
        var i = 0;

        sb.Append('{');
        foreach (var kv in orderedCities)
        {
            if (i < orderedCities.Count - 1)
            {
                sb.Append($"{kv.Key}={(float)kv.Value.Min / 10}/{(float)kv.Value.Sum / 10 / kv.Value.Count:F1}/{(float)kv.Value.Max / 10},");
            }
            else
            {
                sb.Append($"{kv.Key}={(float)kv.Value.Min / 10}/{(float)kv.Value.Sum / 10 / kv.Value.Count:F1}/{(float)kv.Value.Max / 10}");
            }
            i++;
        }
        sb.Append('}');

        Console.WriteLine(sb.ToString());
    }
}

public class MeasurementCalculatorNew
{
    private const string FileName = @"/Users/davidnascimento/source/1brc-net/measurements_1b.txt";
    private const int BufferSize = 4 * 1024 * 1024;      // 4 MiB per thread
    private const int DegreeOfParallelism = 20;          // keep same as your original

    public void CalculateMeasurements()
    {
        var fileInfo = new FileInfo(FileName);
        long fileSize = fileInfo.Length;

        var global = new ConcurrentDictionary<string, Measurement>();

        using var mmf = MemoryMappedFile.CreateFromFile(FileName, FileMode.Open, null, 0L, MemoryMappedFileAccess.Read);

        var tasks = new Task[DegreeOfParallelism];
        var boundaries = ComputeBoundaries(mmf, fileSize, DegreeOfParallelism);

        for (int i = 0; i < DegreeOfParallelism; i++)
        {
            long start = boundaries[i];
            long end = boundaries[i + 1];

            tasks[i] = Task.Run(() =>
            {
                var local = new Dictionary<string, Measurement>(DegreeOfParallelism * 4);

                ProcessChunk(mmf, start, end, local);

                // Merge local -> global (only once per thread)
                foreach (var kv in local)
                {
                    global.AddOrUpdate(kv.Key,
                        kv.Value,
                        (_, existing) =>
                        {
                            existing.Add(kv.Value);
                            return existing;
                        });
                }
            });
        }

        Task.WaitAll(tasks);

        // (Optional) Print
        // PrintToConsole(global);
    }

    private static long[] ComputeBoundaries(MemoryMappedFile mmf, long fileSize, int threads)
    {
        long[] b = new long[threads + 1];
        long chunkSize = fileSize / threads;
        b[0] = 0;
        b[threads] = fileSize;

        using var accessor = mmf.CreateViewAccessor(0, fileSize, MemoryMappedFileAccess.Read);

        for (int i = 1; i < threads; i++)
        {
            long pos = i * chunkSize;
            long offset = 0;
            byte bRead;

            do
            {
                accessor.Read(pos + offset, out bRead);
                offset++;
            } while (bRead != (byte)'\n');

            b[i] = pos + offset;
        }

        return b;
    }

    private static void ProcessChunk(MemoryMappedFile mmf, long start, long end, IDictionary<string, Measurement> dict)
    {
        long length = end - start;
        var buf = new byte[BufferSize];

        using var stream = mmf.CreateViewStream(start, length, MemoryMappedFileAccess.Read);

        int bytesRead;
        int bufferPos = 0;
        long filePos = start;          // file position relative to the chunk
        long lineStart = start;        // start of the current line in the file

        while ((bytesRead = stream.Read(buf, 0, BufferSize)) > 0)
        {
            int i = 0;
            while (i < bytesRead)
            {
                byte b = buf[i];

                if (b == (byte)'\n')
                {
                    // we have a full line from lineStart .. filePos+i-1
                    ParseLine(buf, i, dict);
                    lineStart = filePos + i + 1;
                }

                i++;
            }

            filePos += bytesRead;
        }

        // last line (no trailing \n)
        if (lineStart < end)
            ParseLine(buf, (int)(end - lineStart), dict, true);
    }

    private static void ParseLine(byte[] buf, int length, IDictionary<string, Measurement> dict, bool lastLine = false)
    {
        // Find ';' and '.' inside the line
        int i = 0;
        while (i < length && buf[i] != (byte)';') i++;

        int cityStart = 0;
        int cityLen   = i;

        int mStart = i + 1;
        while (mStart < length && buf[mStart] != (byte)'.') mStart++;

        int intPartStart = mStart;
        int decPartStart = mStart + 1;

        // parse integer part
        int intVal = 0;
        for (int j = cityLen; j < intPartStart; j++)
            intVal = intVal * 10 + (buf[j] - (byte)'0');

        // parse decimal part (exactly one digit in the file)
        int decVal = buf[decPartStart] - (byte)'0';

        int value = intVal * 10 + decVal;   // e.g. 21.3 -> 213

        // build city string *once* per line (cheap)
        string city = System.Text.Encoding.ASCII.GetString(buf, cityStart, cityLen);

        if (!dict.TryGetValue(city, out var meas))
        {
            dict[city] = meas = new Measurement { Min = value, Max = value, Sum = value, Count = 1 };
        }
        else
        {
            meas.Add(value);
        }
    }

    // Helper for merging measurements
    private sealed class Measurement
    {
        public int Min, Max, Sum;
        public ulong Count;

        public void Add(int value)
        {
            if (value < Min) Min = value;
            if (value > Max) Max = value;
            Sum += value;
            Count++;
        }

        public void Add(Measurement other)
        {
            if (other.Min < Min) Min = other.Min;
            if (other.Max > Max) Max = other.Max;
            Sum += other.Sum;
            Count += other.Count;
        }
    }
}


internal class Measurement
{
    public int Min { get; set; }
    public int Max { get; set; }
    public int Sum { get; set; }
    public ulong Count { get; set; }
}

