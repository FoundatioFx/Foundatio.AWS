﻿using System;
using System.Threading.Tasks;
using Amazon;
using Foundatio.Metrics;
using Foundatio.Tests.Metrics;
using Foundatio.Tests.Utility;
using Microsoft.Extensions.Logging;
using Xunit;
using Xunit.Abstractions;

namespace Foundatio.AWS.Tests.Metrics {
    public class CloudWatchMetricsTests : MetricsClientTestBase {
        public CloudWatchMetricsTests(ITestOutputHelper output) : base(output) {
            Log.MinimumLevel = LogLevel.Trace;
        }

        public override IMetricsClient GetMetricsClient(bool buffered = false) {
            // Don't run this as part of the tests because it doesn't work reliably since CloudWatch can take a long time for the stats to show up.	
            // Also, you can't delete metrics so we have to use random ids and it creates a bunch of junk data.	
            return null;
            
#pragma warning disable CS0162 // Unreachable code detected
            string id = Guid.NewGuid().ToString("N").Substring(0, 10);
            return new CloudWatchMetricsClient(
                o => o.ConnectionString($"serviceurl=http://localhost:4566;Test Id={id}")
                    .Prefix("foundatio/tests/metrics")
                    .Buffered(buffered)
                    .LoggerFactory(Log));
#pragma warning restore CS0162 // Unreachable code detected
        }

        [Fact]
        public override Task CanSetGaugesAsync() {
            return base.CanSetGaugesAsync();
        }

        [Fact]
        public override Task CanIncrementCounterAsync() {
            return base.CanIncrementCounterAsync();
        }

        [Fact]
        public override Task CanWaitForCounterAsync() {
            return base.CanWaitForCounterAsync();
        }

        [Fact]
        public override Task CanGetBufferedQueueMetricsAsync() {
            return base.CanGetBufferedQueueMetricsAsync();
        }

        [Fact]
        public override Task CanIncrementBufferedCounterAsync() {
            return base.CanIncrementBufferedCounterAsync();
        }

        [Fact]
        public override Task CanSendBufferedMetricsAsync() {
            return base.CanSendBufferedMetricsAsync();
        }
    }
}