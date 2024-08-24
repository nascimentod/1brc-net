using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Threading.Channels;

internal class Program
{
    private static async Task Main(string[] args)
    {
        BenchmarkRunner.Run<MeasurementCalculator>();
        //await new MeasurementCalculator().CalculateMeasurements();
    }
}

[MemoryDiagnoser]
public class MeasurementCalculator 
{
    [Benchmark]
    public async Task CalculateMeasurements()
    {
        var fileName = "C:/source/1brc-net/measurements.txt";

        var dictionary = new ConcurrentDictionary<string, Measurement>();
        var channel = Channel.CreateUnbounded<string>();

        await Task.WhenAll(WriteTask(fileName, channel.Writer), ReadTask(dictionary, channel.Reader));

        // Console output
        //PrintToConsole(dictionary);
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
                sb.Append($"{kv.Key}={kv.Value.Min}/{kv.Value.Sum / kv.Value.Count:F1}/{kv.Value.Max},");
            }
            else
            {
                sb.Append($"{kv.Key}={kv.Value.Min}/{kv.Value.Sum / kv.Value.Count:F1}/{kv.Value.Max}");
            }
            i++;
        }
        sb.Append('}');

        Console.WriteLine(sb.ToString());
    }

    private async Task WriteTask(string fileName, ChannelWriter<string> writer)
    {
        var sw = new Stopwatch();
        sw.Start();

        using var fs = new FileStream(fileName, FileMode.Open, FileAccess.Read);
        using var fileReader = new StreamReader(fs);
        string? line;
        while ((line = await fileReader.ReadLineAsync()) != null)
        {
            await writer.WriteAsync(line);
        }

        writer.Complete();
        
        sw.Stop();

        Console.WriteLine($"{sw.Elapsed.TotalMilliseconds} ms for write");
    }

    private async Task ReadTask(ConcurrentDictionary<string, Measurement> dictionary, ChannelReader<string> reader)
    {
        var sw = new Stopwatch();
        sw.Start();

        await foreach(var line in reader.ReadAllAsync())
        {
            ProcessLine(dictionary, line);
        }

        sw.Stop();

        Console.WriteLine($"{sw.Elapsed.TotalMilliseconds} ms for read.");
    }

    private static void ProcessLine(ConcurrentDictionary<string, Measurement> dictionary, string line)
    {
        var split = line.Split(';');
        var value = double.Parse(split[1]);

        dictionary.AddOrUpdate(split[0], new Measurement() { Min = value, Sum = value, Max = value, Count = 1 }, (_, measurement) =>
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
            measurement.Count++;

            return measurement;
        });
    }
}

internal struct Measurement
{
    public double Min { get;set; }
    public double Max { get;set; }
    public double Sum { get;set; }
    public ulong Count {get;set;}
}

