using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Foundatio.Caching;
using Foundatio.Queues;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Foundatio.AWS.Tests.Queues;

public class SQSQueueSyntheticTests
{
    [Theory]
    [InlineData("GetQueueAttributes")]
    [InlineData("GetQueueUrl")]
    public async Task AbandonAsync_WhenDeadletterLookupFails_RetainsMessageAsync(string failedOperation)
    {
        // Arrange
        using var transport = new SqsTransport { HasRedrivePolicy = true, FailedOperation = failedOperation };
        using var queue = CreateQueue(transport);
        var entry = CreateEntry(queue);

        // Act
        await Assert.ThrowsAsync<AmazonSQSException>(() => queue.AbandonAsync(entry));

        // Assert
        Assert.False(entry.IsAbandoned);
        Assert.DoesNotContain("DeleteMessage", transport.Operations);
        Assert.DoesNotContain("SendMessage", transport.Operations);
        transport.FailedOperation = null;
        await queue.AbandonAsync(entry);
        Assert.True(entry.IsAbandoned);
        Assert.Equal(new[] { "SendMessage", "DeleteMessage" }, transport.Operations.TakeLast(2));
    }

    [Fact]
    public async Task AbandonAsync_WhenRedrivePolicyIsAdded_UsesNewDeadletterQueueAsync()
    {
        // Arrange
        using var transport = new SqsTransport();
        using var queue = CreateQueue(transport);
        await queue.AbandonAsync(CreateEntry(queue));
        transport.HasRedrivePolicy = true;
        transport.Operations.Clear();

        // Act
        await queue.AbandonAsync(CreateEntry(queue));

        // Assert
        Assert.Equal(new[] { "GetQueueAttributes", "GetQueueUrl", "SendMessage", "DeleteMessage" }, transport.Operations);
    }

    [Fact]
    public async Task AbandonAsync_WithoutRedrivePolicy_DeletesExhaustedMessageAsync()
    {
        // Arrange
        using var transport = new SqsTransport();
        using var queue = CreateQueue(transport);
        var entry = CreateEntry(queue);

        // Act
        await queue.AbandonAsync(entry);

        // Assert
        Assert.True(entry.IsAbandoned);
        Assert.Equal(new[] { "GetQueueAttributes", "DeleteMessage" }, transport.Operations);
    }

    [Theory]
    [InlineData(10, false)]
    [InlineData(9, true)]
    public async Task EnqueueAsync_WithActivityAttributeOverflow_DoesNotReserveDuplicateKeyAsync(int propertyCount, bool traceState)
    {
        // Arrange
        using var activity = new Activity("enqueue").Start();
        activity.TraceStateString = traceState ? "vendor=value" : null;
        using var cache = new InMemoryCacheClient();
        using var transport = new SqsTransport();
        using var queue = CreateQueue(transport);
        queue.AttachBehavior(new DuplicateDetectionQueueBehavior<WorkItem>(cache, NullLoggerFactory.Instance));
        var options = new QueueEntryOptions();
        for (int i = 0; i < propertyCount; i++)
            options.Properties.Add("property" + i, "value");

        // Act
        await Assert.ThrowsAsync<QueueException>(() => queue.EnqueueAsync(new WorkItem(), options));

        // Assert
        Assert.False(await cache.ExistsAsync("work-item"));
        Assert.DoesNotContain("SendMessage", transport.Operations);
    }

    [Fact]
    public async Task EnqueueAsync_WithCaseInsensitivePropertyNames_DoesNotReserveDuplicateKeyAsync()
    {
        // Arrange
        using var cache = new InMemoryCacheClient();
        using var transport = new SqsTransport();
        using var queue = CreateQueue(transport);
        queue.AttachBehavior(new DuplicateDetectionQueueBehavior<WorkItem>(cache, NullLoggerFactory.Instance));
        var options = new QueueEntryOptions
        {
            CorrelationId = "explicit",
            Properties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["correlationid"] = "property" }
        };
        for (int i = 0; i < 9; i++)
            options.Properties.Add("property" + i, "value");

        // Act
        await Assert.ThrowsAsync<QueueException>(() => queue.EnqueueAsync(new WorkItem(), options));

        // Assert
        Assert.False(await cache.ExistsAsync("work-item"));
        Assert.DoesNotContain("SendMessage", transport.Operations);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task EnqueueAsync_WithMetadataNameCollisions_CountsEffectiveAttributesAsync(bool explicitCorrelation)
    {
        // Arrange
        using var activity = new Activity("enqueue").Start();
        activity.TraceStateString = "vendor=value";
        using var transport = new SqsTransport();
        using var queue = CreateQueue(transport);
        var options = new QueueEntryOptions { CorrelationId = explicitCorrelation ? "explicit" : null };
        options.Properties.Add("CorrelationId", "property-correlation");
        options.Properties.Add("TraceState", "property-trace");
        for (int i = 0; i < 8; i++)
            options.Properties.Add("property" + i, "value");
        bool enqueuingCalled = false;
        queue.Enqueuing.AddHandler((_, args) =>
        {
            enqueuingCalled = true;
            args.Cancel = true;
            return Task.CompletedTask;
        });

        // Act
        var id = await queue.EnqueueAsync(new WorkItem(), options);

        // Assert
        Assert.Null(id);
        Assert.True(enqueuingCalled);
        Assert.DoesNotContain("SendMessage", transport.Operations);
    }

    [Fact]
    public async Task StartWorkingAsync_WhenDeadletterLookupFails_KeepsWorkingAndRetriesAsync()
    {
        // Arrange: the first dead-letter lookup for an exhausted message fails, the redelivered copy succeeds
        using var transport = new SqsTransport
        {
            HasRedrivePolicy = true,
            FailedOperation = "GetQueueAttributes",
            FailuresRemaining = 1,
            ExhaustedReceivesRemaining = 2
        };
        using var queue = CreateQueue(transport);
        using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        // Act
        await queue.StartWorkingAsync((_, _) => Task.CompletedTask, cancellationToken: cancellationTokenSource.Token);
        await transport.MessageDeleted.Task.WaitAsync(cancellationTokenSource.Token);

        // Assert
        Assert.Equal(1, transport.FailedRequests);
        Assert.Contains("SendMessage", transport.Operations);
    }

    private static SQSQueue<WorkItem> CreateQueue(SqsTransport transport) => new(new SQSQueueOptions<WorkItem>
    {
        Name = "source",
        Credentials = new AnonymousAWSCredentials(),
        ServiceUrl = "https://sqs.test.invalid",
        HttpClientFactory = transport,
        Retries = 0,
        DequeueInterval = TimeSpan.FromMilliseconds(10),
        MetricsPollingEnabled = false
    });

    private static SQSQueueEntry<WorkItem> CreateEntry(SQSQueue<WorkItem> queue) => new(new Message
    {
        MessageId = "message",
        ReceiptHandle = "receipt",
        Body = "{}",
        Attributes = new Dictionary<string, string> { ["ApproximateReceiveCount"] = "1" }
    }, new WorkItem(), queue);

    public class WorkItem : IHaveUniqueIdentifier
    {
        public string UniqueIdentifier => "work-item";
    }

    private sealed class SqsTransport : HttpClientFactory, IDisposable
    {
        private readonly HttpClient _client;
        public bool HasRedrivePolicy { get; set; }
        public string? FailedOperation { get; set; }
        public int FailuresRemaining { get; set; } = Int32.MaxValue;
        public int FailedRequests { get; private set; }
        public int ExhaustedReceivesRemaining { get; set; }
        public List<string> Operations { get; } = new();
        public TaskCompletionSource MessageDeleted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public SqsTransport() => _client = new HttpClient(new Handler(this));
        public override HttpClient CreateHttpClient(IClientConfig clientConfig) => _client;
        public void Dispose() => _client.Dispose();

        private sealed class Handler(SqsTransport transport) : HttpMessageHandler
        {
            protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            {
                string operation = request.Headers.GetValues("X-Amz-Target").Single().Split('.').Last();
                transport.Operations.Add(operation);
                bool failed = operation == transport.FailedOperation && transport.FailuresRemaining > 0;
                if (failed)
                {
                    transport.FailuresRemaining--;
                    transport.FailedRequests++;
                }

                string response = failed ? "{\"__type\":\"AccessDeniedException\",\"message\":\"Synthetic lookup failure\"}" : operation switch
                {
                    "GetQueueUrl" => "{\"QueueUrl\":\"https://sqs.test.invalid/queue\"}",
                    "GetQueueAttributes" => JsonSerializer.Serialize(new
                    {
                        Attributes = transport.HasRedrivePolicy
                            ? new Dictionary<string, string> { ["RedrivePolicy"] = "{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:source-deadletter\",\"maxReceiveCount\":\"1\"}" }
                            : new Dictionary<string, string>()
                    }),
                    "ReceiveMessage" => transport.ExhaustedReceivesRemaining-- > 0
                        ? "{\"Messages\":[{\"MessageId\":\"message\",\"ReceiptHandle\":\"receipt\",\"Body\":\"{}\",\"MD5OfBody\":\"99914b932bd37a50b983c5e7c90ae93b\",\"Attributes\":{\"ApproximateReceiveCount\":\"5\"}}]}"
                        : "{\"Messages\":[]}",
                    "SendMessage" => "{\"MessageId\":\"dead-message\",\"MD5OfMessageBody\":\"99914b932bd37a50b983c5e7c90ae93b\"}",
                    "DeleteMessage" => "{}",
                    _ => throw new InvalidOperationException("Unexpected synthetic SDK request: " + operation)
                };
                if (operation == "DeleteMessage")
                    transport.MessageDeleted.TrySetResult();
                return Task.FromResult(new HttpResponseMessage(failed ? HttpStatusCode.BadRequest : HttpStatusCode.OK)
                {
                    Content = new StringContent(response, Encoding.UTF8, "application/x-amz-json-1.0")
                });
            }
        }
    }
}
