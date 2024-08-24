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
        var fileName = "C:/source/1brc-net/measurements.txt";
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

                do
                {
                    offset++;
                    accessor.Read(start + offset, out character);
                }while(character != '\n'); // While we don't hit a new line character, we keep moving forward

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
        using var reader = new BinaryReader(stream);
        var sb = new StringBuilder();

        try
        {
            while(true)
            {
                var character = reader.ReadChar();

                if(character == '\n')
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
        catch(EndOfStreamException)
        {
            // END
        }
    }

    private static void ProcessLine(ConcurrentDictionary<string, Measurement> dictionary, string line)
    {
        if(string.IsNullOrWhiteSpace(line))
        {
            return;
        }

        try
        {
            // Get city name
            int i = 0;
            while(i < line.Length && line[i] != ';')
            {
                i++;
            }

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

