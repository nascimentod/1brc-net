using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using System.Globalization;
using System.Text;

internal class Program
{
    private static async Task Main(string[] args)
    {
        //BenchmarkRunner.Run<MeasurementCalculator>();
        await new MeasurementCalculator().CalculateMeasurements();
    }
}

[MemoryDiagnoser]
public class MeasurementCalculator 
{
    [Benchmark]
    public async Task CalculateMeasurements()
    {
        var fileName = "C:/source/1brc-net/measurements.txt";

        var dictionary = new Dictionary<string, Measurement>();

        using var fs = new FileStream(fileName, FileMode.Open, FileAccess.Read);
        using var reader = new StreamReader(fs);
        string? line;
        while ((line = await reader.ReadLineAsync()) != null)
        {
            var split = line.Split(';');
            var city = split[0];
            var value = double.Parse(split[1]);
            
            if(dictionary.TryGetValue(split[0], out var measurement)) 
            {
                if(measurement.Min > value)
                {
                    measurement.Min = value;
                }
                if(measurement.Max < value)
                {
                    measurement.Max = value;
                }
                measurement.Sum += value;
                measurement.Count += 1;
            }
            else
            {
                dictionary.Add(split[0], new Measurement(){Min = value, Sum = value, Max = value, Count = 1});
            }
        }

        var orderedCities = dictionary.OrderBy(x => x.Key).ToDictionary();

        var sb = new StringBuilder();
        var i = 0;
        
        sb.Append("{");
        foreach(var kv in orderedCities)
        {
            if(i < orderedCities.Count - 1) 
            {
                sb.Append($"{kv.Key}={kv.Value.Min}/{kv.Value.Sum/kv.Value.Count:F1}/{kv.Value.Max},");
            }
            else
            {
                sb.Append($"{kv.Key}={kv.Value.Min}/{kv.Value.Sum/kv.Value.Count:F1}/{kv.Value.Max}");
            }
            i++;
        }
        sb.Append("}");

        Console.WriteLine(sb.ToString());
    }
}

internal class Measurement
{
    public double Min { get;set; }
    public double Max { get;set; }
    public double Sum { get;set; }
    public ulong Count {get;set;}
}

