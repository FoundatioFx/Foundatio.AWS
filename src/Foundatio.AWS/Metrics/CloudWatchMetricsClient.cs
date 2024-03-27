﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.CloudWatch;
using Amazon.CloudWatch.Model;
using Amazon.Runtime;
using Foundatio.Extensions;
using Foundatio.Utility;
using Microsoft.Extensions.Logging;

namespace Foundatio.Metrics;

public class CloudWatchMetricsClient : BufferedMetricsClientBase, IMetricsClientStats
{
    private readonly Lazy<AmazonCloudWatchClient> _client;
    private readonly CloudWatchMetricsClientOptions _options;
    private readonly string _namespace;
    private readonly List<Dimension> _dimensions;

    public CloudWatchMetricsClient(CloudWatchMetricsClientOptions options) : base(options)
    {
        _options = options;
        _namespace = options.Namespace;
        _dimensions = options.Dimensions;
        _client = new Lazy<AmazonCloudWatchClient>(() => new AmazonCloudWatchClient(
            options.Credentials ?? FallbackCredentialsFactory.GetCredentials(),
            new AmazonCloudWatchConfig
            {
                LogResponse = false,
                DisableLogging = true,
                RegionEndpoint = options.Region ?? FallbackRegionFactory.GetRegionEndpoint()
            }));
    }

    public CloudWatchMetricsClient(Builder<CloudWatchMetricsClientOptionsBuilder, CloudWatchMetricsClientOptions> builder)
        : this(builder(new CloudWatchMetricsClientOptionsBuilder()).Build()) { }

    public AmazonCloudWatchClient Client => _client.Value;

    protected override async Task StoreAggregatedMetricsAsync(TimeBucket timeBucket, ICollection<AggregatedCounterMetric> counters, ICollection<AggregatedGaugeMetric> gauges, ICollection<AggregatedTimingMetric> timings)
    {
        var metrics = new List<MetricDatum>();
        metrics.AddRange(ConvertToDatums(counters));
        metrics.AddRange(ConvertToDatums(gauges));
        metrics.AddRange(ConvertToDatums(timings));

        int page = 1;
        int pageSize = 20; // CloudWatch only allows max 20 metrics at once.

        do
        {
            var metricsPage = metrics.Skip((page - 1) * pageSize).Take(pageSize).ToList();
            if (metricsPage.Count == 0)
                break;

            if (_logger.IsEnabled(LogLevel.Trace)) _logger.LogTrace("Sending PutMetricData to AWS for {Count} metric(s)", metricsPage.Count);
            // do retries
            var response = await _client.Value.PutMetricDataAsync(new PutMetricDataRequest
            {
                Namespace = _namespace,
                MetricData = metricsPage
            }).AnyContext();

            if (response.HttpStatusCode != System.Net.HttpStatusCode.OK)
                throw new AmazonCloudWatchException("Unable to post metrics.");

            page++;
        } while (true);
    }

    private IEnumerable<MetricDatum> ConvertToDatums(ICollection<AggregatedCounterMetric> counters)
    {
        foreach (var counter in counters)
        {
            yield return new MetricDatum
            {
                TimestampUtc = counter.Key.StartTimeUtc,
                MetricName = GetMetricName(MetricType.Counter, counter.Key.Name),
                Value = counter.Value
            };

            yield return new MetricDatum
            {
                Dimensions = _dimensions,
                TimestampUtc = counter.Key.StartTimeUtc,
                MetricName = GetMetricName(MetricType.Counter, counter.Key.Name),
                Value = counter.Value
            };
        }
    }

    private IEnumerable<MetricDatum> ConvertToDatums(ICollection<AggregatedGaugeMetric> gauges)
    {
        foreach (var gauge in gauges)
        {
            yield return new MetricDatum
            {
                TimestampUtc = gauge.Key.StartTimeUtc,
                MetricName = GetMetricName(MetricType.Gauge, gauge.Key.Name),
                StatisticValues = new StatisticSet
                {
                    SampleCount = gauge.Count,
                    Sum = gauge.Total,
                    Minimum = gauge.Min,
                    Maximum = gauge.Max
                }
            };

            yield return new MetricDatum
            {
                Dimensions = _dimensions,
                TimestampUtc = gauge.Key.StartTimeUtc,
                MetricName = GetMetricName(MetricType.Gauge, gauge.Key.Name),
                StatisticValues = new StatisticSet
                {
                    SampleCount = gauge.Count,
                    Sum = gauge.Total,
                    Minimum = gauge.Min,
                    Maximum = gauge.Max
                }
            };
        }
    }

    private IEnumerable<MetricDatum> ConvertToDatums(ICollection<AggregatedTimingMetric> timings)
    {
        foreach (var timing in timings)
        {
            yield return new MetricDatum
            {
                TimestampUtc = timing.Key.StartTimeUtc,
                MetricName = GetMetricName(MetricType.Timing, timing.Key.Name),
                StatisticValues = new StatisticSet
                {
                    SampleCount = timing.Count,
                    Sum = timing.TotalDuration,
                    Minimum = timing.MinDuration,
                    Maximum = timing.MaxDuration
                },
                Unit = StandardUnit.Milliseconds
            };

            yield return new MetricDatum
            {
                Dimensions = _dimensions,
                TimestampUtc = timing.Key.StartTimeUtc,
                MetricName = GetMetricName(MetricType.Timing, timing.Key.Name),
                StatisticValues = new StatisticSet
                {
                    SampleCount = timing.Count,
                    Sum = timing.TotalDuration,
                    Minimum = timing.MinDuration,
                    Maximum = timing.MaxDuration
                },
                Unit = StandardUnit.Milliseconds
            };
        }
    }

    private string GetMetricName(MetricType metricType, string name)
    {
        return String.Concat(_options.Prefix, name);
    }

    private int GetStatsPeriod(DateTime start, DateTime end)
    {
        double totalMinutes = end.Subtract(start).TotalMinutes;
        var interval = TimeSpan.FromMinutes(1);
        if (totalMinutes >= 60 * 24 * 7)
            interval = TimeSpan.FromDays(1);
        else if (totalMinutes >= 60 * 2)
            interval = TimeSpan.FromMinutes(5);

        return (int)interval.TotalSeconds;
    }

    public async Task<CounterStatSummary> GetCounterStatsAsync(string name, DateTime? start = default(DateTime?), DateTime? end = default(DateTime?), int dataPoints = 20)
    {
        if (!start.HasValue)
            start = SystemClock.UtcNow.AddHours(-4);

        if (!end.HasValue)
            end = SystemClock.UtcNow;

        var request = new GetMetricStatisticsRequest
        {
            Namespace = _namespace,
            MetricName = GetMetricName(MetricType.Counter, name),
            Period = GetStatsPeriod(start.Value, end.Value),
            StartTimeUtc = start.Value,
            EndTimeUtc = end.Value,
            Statistics = new List<string> { "Sum" }
        };

        var response = await _client.Value.GetMetricStatisticsAsync(request).AnyContext();
        if (response.HttpStatusCode != System.Net.HttpStatusCode.OK)
            throw new AmazonCloudWatchException("Unable to retrieve metrics.");

        return new CounterStatSummary(
            name,
            response.Datapoints.Select(dp => new CounterStat
            {
                Count = (long)dp.Sum,
                Time = dp.Timestamp
            }).ToList(),
            start.Value,
            end.Value);
    }

    public async Task<GaugeStatSummary> GetGaugeStatsAsync(string name, DateTime? start = default(DateTime?), DateTime? end = default(DateTime?), int dataPoints = 20)
    {
        if (!start.HasValue)
            start = SystemClock.UtcNow.AddHours(-4);

        if (!end.HasValue)
            end = SystemClock.UtcNow;

        var request = new GetMetricStatisticsRequest
        {
            Namespace = _namespace,
            MetricName = GetMetricName(MetricType.Counter, name),
            Period = GetStatsPeriod(start.Value, end.Value),
            StartTimeUtc = start.Value,
            EndTimeUtc = end.Value,
            Statistics = new List<string> { "Sum", "Minimum", "Maximum", "SampleCount", "Average" }
        };

        var response = await _client.Value.GetMetricStatisticsAsync(request).AnyContext();
        if (response.HttpStatusCode != System.Net.HttpStatusCode.OK)
            throw new AmazonCloudWatchException("Unable to retrieve metrics.");

        return new GaugeStatSummary(
            name,
            response.Datapoints.Select(dp => new GaugeStat
            {
                Max = dp.Maximum,
                Min = dp.Minimum,
                Total = dp.Sum,
                Time = dp.Timestamp,
                Count = (int)dp.SampleCount,
                Last = dp.Average
            }).ToList(),
            start.Value,
            end.Value);
    }

    public async Task<TimingStatSummary> GetTimerStatsAsync(string name, DateTime? start = default(DateTime?), DateTime? end = default(DateTime?), int dataPoints = 20)
    {
        if (!start.HasValue)
            start = SystemClock.UtcNow.AddHours(-4);

        if (!end.HasValue)
            end = SystemClock.UtcNow;

        var request = new GetMetricStatisticsRequest
        {
            Namespace = _namespace,
            MetricName = GetMetricName(MetricType.Counter, name),
            Period = GetStatsPeriod(start.Value, end.Value),
            StartTimeUtc = start.Value,
            EndTimeUtc = end.Value,
            Unit = StandardUnit.Milliseconds,
            Statistics = new List<string> { "Sum", "Minimum", "Maximum", "SampleCount" }
        };

        var response = await _client.Value.GetMetricStatisticsAsync(request).AnyContext();
        if (response.HttpStatusCode != System.Net.HttpStatusCode.OK)
            throw new AmazonCloudWatchException("Unable to retrieve metrics.");

        return new TimingStatSummary(
            name,
            response.Datapoints.Select(dp => new TimingStat
            {
                MinDuration = (int)dp.Minimum,
                MaxDuration = (int)dp.Maximum,
                TotalDuration = (long)dp.Sum,
                Count = (int)dp.SampleCount,
                Time = dp.Timestamp
            }).ToList(),
            start.Value,
            end.Value);
    }
}
