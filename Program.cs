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
        new MeasurementCalculator().CalculateMeasurements();
    }
}

[MemoryDiagnoser]
public class MeasurementCalculator 
{
    [Benchmark]
    public void CalculateMeasurements()
    {
        var fileName = "C:/source/1brc-net/measurements_big.txt";
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

        using(var accessor = mmf.CreateViewAccessor(0L, fileSize, MemoryMappedFileAccess.Read))
        {
            for(var i = 1; i < degreeOfParallelisation; i++)
            {
                var start = i * chunkSize;
                var offset = -1;
                byte character;

                int blockSize = 4096;
                while (true)
                {
                    if (start + offset + blockSize > fileSize) blockSize = (int)(fileSize - start - offset);
                    
                    Memory<byte> buffer = new byte[blockSize];
                    accessor.ReadArray(start + offset, buffer.Span, blockSize);

                    int newlineIndex = buffer.IndexOf((byte)'\n');
                    if (newlineIndex != -1)
                    {
                        offset += newlineIndex + 1;
                        break;
                    }
                    else
                    {
                        offset += blockSize;
                    }
                }

                chunkBoundaries[i] = start + offset;
            }
        }

        for(var i = 0; i < degreeOfParallelisation; i++)
        {
            var start = chunkBoundaries[i];
            var end = chunkBoundaries[i+1];
            tasks[i] = Task.Run(() => ProcessChunk(mmf, start, end, dictionary));
        }

        Task.WaitAll(tasks);
    }

    private void ProcessChunk(MemoryMappedFile mmf, long start, long end, ConcurrentDictionary<string, Measurement> dictionary)
    {
        using var stream = mmf.CreateViewStream(start, end - start);
        
        const int bufferSize = 8192;
        byte[] buffer = new byte[bufferSize];
        int bytesRead;

        try
        {
            while ((bytesRead = stream.Read(buffer, 0, bufferSize)) > 0)
            {
                var memory = new ReadOnlyMemory<byte>(buffer, 0, bytesRead);
                ProcessBuffer(memory.Span, dictionary);
            }
        }
        catch(EndOfStreamException)
        {
            // END
        }
    }

    private static void ProcessLine(ConcurrentDictionary<string, Measurement> dictionary, ReadOnlySpan<byte> lineBytes)
    {
        if (lineBytes.IsEmpty || lineBytes[0] == '\n' || lineBytes[0] == '\r')
        {
            return;
        }

        try
        {
            intsemicolonIndex = lineBytes.IndexOf((byte)';');
            if (semicolonIndex == -1)
                return;

            var citySpan = lineBytes.Slice(0, semicolonIndex);

            var lineSpan = line.AsSpan();
            var city = lineSpan[..i];
            var measurement = lineSpan[(i+1)..];
            
            i = 0;
            // Get measurement integer and decimal parts
            while(i < measurement.Length && measurement[i] != '.')
            {
                i++;
            }
            var integerPart = measurement[..i];
            var decimalPart = measurement[(i+1)..];

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
        catch(Exception ex)
        {
            Console.WriteLine($"{line} was the problem");
        }
    }

    private static void ProcessBuffer(RoSpan<byte> buffer, ConcurrentDictionary<string, Measurement> dictionary)
    {
        int lineStart = 0;
        
        for (int i = 0; i < buffer.Length; i++)
        {
            if (buffer[i] == '\n')
            {
                var lineSpan = buffer.Slice(lineStart, i - lineStart + 1);
                ProcessLine(dictionary, lineSpan);
                lineStart = i + 1;
            }
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

internal class Measurement
{
    public int Min { get;set; }
    public int Max { get;set; }
    public int Sum { get;set; }
    public ulong Count {get;set;}
}

