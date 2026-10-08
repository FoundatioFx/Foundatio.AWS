using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using Foundatio.Queues;
using Foundatio.Serializer;
using Foundatio.Tests.Queue;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Foundatio.AWS.Tests.Queues;

public class SQSQueueTests : QueueTestBase
{
    private readonly string _queueName = "foundatio-" + Guid.NewGuid().ToString("N").Substring(10);

    public SQSQueueTests(ITestOutputHelper output) : base(output)
    {
        // SQS queue stats are approximate and unreliable
        _assertStats = false;
    }

    protected override IQueue<SimpleWorkItem>? GetQueue(int retries = 1, TimeSpan? workItemTimeout = null,
        TimeSpan? retryDelay = null, int[]? retryMultipliers = null, int deadLetterMaxItems = 100,
        bool runQueueMaintenance = true, TimeProvider? timeProvider = null, ISerializer? serializer = null)
    {
        var queue = new SQSQueue<SimpleWorkItem>(o => o
            .ConnectionString("serviceurl=http://localhost:4566;AccessKey=xxx;SecretKey=xxx")
            .Name(_queueName)
            .Retries(retries)
            .RetryDelay(attempt =>
            {
                int[] multipliers = retryMultipliers ?? [1, 3, 5, 10];
                int index = Math.Min(attempt, multipliers.Length - 1);
                return TimeSpan.FromSeconds(multipliers[index]);
            })
            .WorkItemTimeout(workItemTimeout.GetValueOrDefault(TimeSpan.FromMinutes(5)))
            .DequeueInterval(TimeSpan.FromSeconds(1))
            .ReadQueueTimeout(TimeSpan.FromSeconds(1))
            .MetricsPollingInterval(TimeSpan.Zero)
            .TimeProvider(timeProvider)
            .Serializer(serializer)
            .LoggerFactory(Log));

        _logger.LogDebug("Queue Id: {QueueId}", queue.QueueId);
        return queue;
    }

    protected IQueue<SimpleWorkItem> GetQueue(int retries = 1, TimeSpan? workItemTimeout = null,
        TimeSpan? retryDelay = null, int[]? retryMultipliers = null, int deadLetterMaxItems = 100,
        bool runQueueMaintenance = true, TimeSpan? dequeueInterval = null, TimeSpan? readQueueTimeout = null)
    {
        var queue = new SQSQueue<SimpleWorkItem>(o => o
            .ConnectionString("serviceurl=http://localhost:4566;AccessKey=xxx;SecretKey=xxx")
            .Name(_queueName)
            .Retries(retries)
            .RetryDelay(attempt =>
            {
                int[] multipliers = retryMultipliers ?? [1, 3, 5, 10];
                int index = Math.Min(attempt, multipliers.Length - 1);
                return TimeSpan.FromSeconds(multipliers[index]);
            })
            .WorkItemTimeout(workItemTimeout.GetValueOrDefault(TimeSpan.FromMinutes(5)))
            .DequeueInterval(dequeueInterval ?? TimeSpan.FromSeconds(1))
            .ReadQueueTimeout(readQueueTimeout ?? TimeSpan.FromSeconds(1))
            .MetricsPollingInterval(TimeSpan.Zero)
            .LoggerFactory(Log));

        _logger.LogDebug("Queue Id: {QueueId}", queue.QueueId);
        return queue;
    }

    [Fact]
    public override Task CanQueueAndDequeueWorkItemAsync()
    {
        return base.CanQueueAndDequeueWorkItemAsync();
    }

    [Fact]
    public override Task CanQueueAndDequeueWorkItemWithDelayAsync()
    {
        return base.CanQueueAndDequeueWorkItemWithDelayAsync();
    }

    [Fact]
    public override Task CanUseQueueOptionsAsync()
    {
        return base.CanUseQueueOptionsAsync();
    }

    [Fact]
    public override Task CanDiscardDuplicateQueueEntriesAsync()
    {
        return base.CanDiscardDuplicateQueueEntriesAsync();
    }

    [Fact]
    public override Task CanDequeueWithCancelledTokenAsync()
    {
        return base.CanDequeueWithCancelledTokenAsync();
    }

    [Fact]
    public override Task CanDequeueEfficientlyAsync()
    {
        return base.CanDequeueEfficientlyAsync();
    }

    [Fact]
    public override Task CanResumeDequeueEfficientlyAsync()
    {
        return base.CanResumeDequeueEfficientlyAsync();
    }

    [Fact]
    public override Task CanQueueAndDequeueMultipleWorkItemsAsync()
    {
        return base.CanQueueAndDequeueMultipleWorkItemsAsync();
    }

    [Fact]
    public override Task WillNotWaitForItemAsync()
    {
        return base.WillNotWaitForItemAsync();
    }

    [Fact]
    public override Task WillWaitForItemAsync()
    {
        return base.WillWaitForItemAsync();
    }

    [Fact]
    public override Task DequeueAsync_AfterAbandonWithMutatedValue_ReturnsOriginalValueAsync()
    {
        return base.DequeueAsync_AfterAbandonWithMutatedValue_ReturnsOriginalValueAsync();
    }

    [Fact]
    public override Task DequeueAsync_WithDispose_AutoAbandonsEntryAsync()
    {
        return base.DequeueAsync_WithDispose_AutoAbandonsEntryAsync();
    }

    [Fact]
    public override Task DequeueWaitWillGetSignaledAsync()
    {
        return base.DequeueWaitWillGetSignaledAsync();
    }

    [Fact]
    public override Task DequeueAsync_WithPoisonMessage_MovesToDeadletterAsync()
    {
        return base.DequeueAsync_WithPoisonMessage_MovesToDeadletterAsync();
    }

    [Fact]
    public override Task DuplicateDetection_WithDifferentIdentifiers_AcceptsBothItemsAsync()
    {
        return base.DuplicateDetection_WithDifferentIdentifiers_AcceptsBothItemsAsync();
    }

    [Fact]
    public override Task DuplicateDetection_WithExpiredWindow_AcceptsDuplicateAsync()
    {
        return base.DuplicateDetection_WithExpiredWindow_AcceptsDuplicateAsync();
    }

    [Fact]
    public override Task DuplicateDetection_WithNullIdentifier_AcceptsAllItemsAsync()
    {
        return base.DuplicateDetection_WithNullIdentifier_AcceptsAllItemsAsync();
    }

    [Fact]
    public override Task EnqueueAsync_WithSerializationError_ThrowsAndLeavesQueueEmptyAsync()
    {
        return base.EnqueueAsync_WithSerializationError_ThrowsAndLeavesQueueEmptyAsync();
    }

    [Fact]
    public override Task EnqueueAsync_WithReusedOptions_DoesNotChangeCallerOptionsAsync()
    {
        return base.EnqueueAsync_WithReusedOptions_DoesNotChangeCallerOptionsAsync();
    }

    [Fact(Skip = "SQS does not support custom entry IDs")]
    public override Task EnqueueAsync_WithUniqueId_UsesProvidedIdAsync()
    {
        return base.EnqueueAsync_WithUniqueId_UsesProvidedIdAsync();
    }

    [Fact(Skip = "SQS does not support retrieving deadletter items")]
    public override Task GetDeadletterItemsAsync_WithDeadletteredEntry_ReturnsItemsAsync()
    {
        return base.GetDeadletterItemsAsync_WithDeadletteredEntry_ReturnsItemsAsync();
    }

    [Fact]
    public override Task GetQueueActivity_AfterEnqueueAndDequeue_ReturnsTimestampsAsync()
    {
        return base.GetQueueActivity_AfterEnqueueAndDequeue_ReturnsTimestampsAsync();
    }

    [Fact]
    public override Task GetQueueEntryMetadata_AfterDequeue_ReturnsValidTimestampsAsync()
    {
        return base.GetQueueEntryMetadata_AfterDequeue_ReturnsValidTimestampsAsync();
    }

    [Fact]
    public override Task CanUseQueueWorkerAsync()
    {
        return base.CanUseQueueWorkerAsync();
    }

    [Fact]
    public override Task CanHandleErrorInWorkerAsync()
    {
        return base.CanHandleErrorInWorkerAsync();
    }

    [Fact]
    public override Task StartWorkingAsync_WhenDequeueThrows_KeepsWorkingAsync()
    {
        return base.StartWorkingAsync_WhenDequeueThrows_KeepsWorkingAsync();
    }

    [Fact]
    public override Task StartWorkingAsync_WhenAbandonThrows_KeepsWorkingAsync()
    {
        return base.StartWorkingAsync_WhenAbandonThrows_KeepsWorkingAsync();
    }

    [Fact]
    public override Task StartWorkingAsync_WhenCancelled_StopsWithoutWorkerErrorsAsync()
    {
        return base.StartWorkingAsync_WhenCancelled_StopsWithoutWorkerErrorsAsync();
    }

    [Fact]
    public override Task WorkItemsWillTimeoutAsync()
    {
        return base.WorkItemsWillTimeoutAsync();
    }

    [Fact]
    public override Task WorkItemsWillGetMovedToDeadletterAsync()
    {
        return base.WorkItemsWillGetMovedToDeadletterAsync();
    }

    [Fact]
    public override Task CanAutoCompleteWorkerAsync()
    {
        return base.CanAutoCompleteWorkerAsync();
    }

    [Fact]
    public override Task CanHaveMultipleQueueInstancesAsync()
    {
        return base.CanHaveMultipleQueueInstancesAsync();
    }

    [Fact]
    public override Task CanDelayRetryAsync()
    {
        return base.CanDelayRetryAsync();
    }

    [Fact]
    public override Task CanRunWorkItemWithMetricsAsync()
    {
        return base.CanRunWorkItemWithMetricsAsync();
    }

    [Fact]
    public override Task CanRenewLockAsync()
    {
        return base.CanRenewLockAsync();
    }

    [Fact]
    public override Task CanAbandonQueueEntryOnceAsync()
    {
        return base.CanAbandonQueueEntryOnceAsync();
    }

    [Fact]
    public override Task CanCompleteQueueEntryOnceAsync()
    {
        return base.CanCompleteQueueEntryOnceAsync();
    }

    [Fact]
    public override Task CanDequeueWithLockingAsync()
    {
        return base.CanDequeueWithLockingAsync();
    }

    [Fact]
    public override Task CanHaveMultipleQueueInstancesWithLockingAsync()
    {
        return base.CanHaveMultipleQueueInstancesWithLockingAsync();
    }

    [Fact]
    public override Task MaintainJobNotAbandon_NotWorkTimeOutEntry()
    {
        return base.MaintainJobNotAbandon_NotWorkTimeOutEntry();
    }

    [Fact]
    public override Task QueueEntry_EntryType_ReturnsCorrectTypeAsync()
    {
        return base.QueueEntry_EntryType_ReturnsCorrectTypeAsync();
    }

    [Fact]
    public override Task QueueEntry_GetValue_ReturnsUntypedValueAsync()
    {
        return base.QueueEntry_GetValue_ReturnsUntypedValueAsync();
    }

    [Fact]
    public override Task VerifyRetryAttemptsAsync()
    {
        return base.VerifyRetryAttemptsAsync();
    }

    [Fact]
    public override Task VerifyDelayedRetryAttemptsAsync()
    {
        return base.VerifyDelayedRetryAttemptsAsync();
    }

    [Fact(Skip = "SQS Queues has no queue stats for abandoned, it just increments the queued count and decrements the working count. Only the entry attribute ApproximateNumberOfMessages is available.")]
    public override Task CanHandleAutoAbandonInWorker()
    {
        return base.CanHandleAutoAbandonInWorker();
    }

    [Fact]
    public void RetryBackoff()
    {
        var options = new SQSQueueOptions<SimpleWorkItem>();
        var backoff1 = options.RetryDelay(1);
        Assert.InRange(backoff1, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(3));
        var backoff2 = options.RetryDelay(2);
        Assert.InRange(backoff2, TimeSpan.FromSeconds(4), TimeSpan.FromSeconds(5));
        var backoff3 = options.RetryDelay(3);
        Assert.InRange(backoff3, TimeSpan.FromSeconds(8), TimeSpan.FromSeconds(9));
        var backoff4 = options.RetryDelay(4);
        Assert.InRange(backoff4, TimeSpan.FromSeconds(16), TimeSpan.FromSeconds(17));
        var backoff5 = options.RetryDelay(5);
        Assert.InRange(backoff5, TimeSpan.FromSeconds(32), TimeSpan.FromSeconds(33));
        var backoff6 = options.RetryDelay(6);
        Assert.InRange(backoff6, TimeSpan.FromSeconds(64), TimeSpan.FromSeconds(65));
        var backoff7 = options.RetryDelay(7);
        Assert.InRange(backoff7, TimeSpan.FromSeconds(128), TimeSpan.FromSeconds(129));
        var backoff8 = options.RetryDelay(8);
        Assert.InRange(backoff8, TimeSpan.FromSeconds(256), TimeSpan.FromSeconds(257));
        var backoff9 = options.RetryDelay(9);
        Assert.InRange(backoff9, TimeSpan.FromSeconds(512), TimeSpan.FromSeconds(513));
        var backoff10 = options.RetryDelay(10);
        Assert.InRange(backoff10, TimeSpan.FromSeconds(1024), TimeSpan.FromSeconds(1025));
    }

    [Fact]
    public async Task CanGetQueueItemWithDeliveryDelayAndEnsureMessageNotMarkedWorkingWhileWaiting()
    {
        var queue = GetQueue(dequeueInterval: TimeSpan.Zero, readQueueTimeout: TimeSpan.FromSeconds(2));
        if (queue == null)
            return;

        try
        {
            await queue.DeleteQueueAsync();
            // await AssertEmptyQueueAsync(queue); // TODO: Uncomment once foundatio is updated.

            await queue.EnqueueAsync(new SimpleWorkItem { Data = "Hello" }, new QueueEntryOptions { DeliveryDelay = TimeSpan.FromSeconds(4) });
            var workItem = await queue.DequeueAsync(TimeSpan.FromSeconds(2));
            Assert.Null(workItem);

            if (_assertStats)
            {
                var stats = await queue.GetQueueStatsAsync();
                Assert.Equal(0, stats.Dequeued);
                Assert.Equal(1, stats.Enqueued);
                Assert.Equal(0, stats.Queued);
                Assert.Equal(0, stats.Working);
            }

            await Task.Delay(TimeSpan.FromSeconds(3), TestCancellationToken);

            if (_assertStats)
            {
                var stats = await queue.GetQueueStatsAsync();
                Assert.Equal(0, stats.Dequeued);
                Assert.Equal(1, stats.Enqueued);
                Assert.Equal(1, stats.Queued);
                Assert.Equal(0, stats.Working);
            }

            _logger.LogInformation("Second Dequeue Attempt");
            workItem = await queue.DequeueAsync(TimeSpan.FromSeconds(2));
            Assert.NotNull(workItem);
            Assert.NotNull(workItem?.Value);
            Assert.Equal("Hello", workItem!.Value!.Data);

            if (_assertStats)
            {
                var stats = await queue.GetQueueStatsAsync();
                Assert.Equal(1, stats.Dequeued);
                Assert.Equal(1, stats.Enqueued);
                Assert.Equal(0, stats.Queued);
                Assert.Equal(1, stats.Working);
            }
        }
        finally
        {
            await CleanupQueueAsync(queue);
        }
    }

    [Fact]
    public async Task CanGetQueueItemWithTimeoutCancelledTokenWithDeliveryDelayMaxWaitOverlap()
    {
        var queue = GetQueue(dequeueInterval: TimeSpan.Zero, readQueueTimeout: TimeSpan.FromSeconds(2));
        if (queue == null)
            return;

        try
        {
            await queue.DeleteQueueAsync();
            // await AssertEmptyQueueAsync(queue); // TODO: Uncomment once foundatio is updated.

            await queue.EnqueueAsync(new SimpleWorkItem { Data = "Hello" }, new QueueEntryOptions { DeliveryDelay = TimeSpan.FromSeconds(3) });
            var workItem = await queue.DequeueAsync(TimeSpan.FromSeconds(3));
            Assert.NotNull(workItem);
            Assert.NotNull(workItem?.Value);
            Assert.Equal("Hello", workItem!.Value!.Data);

            if (_assertStats)
            {
                var stats = await queue.GetQueueStatsAsync();
                Assert.Equal(1, stats.Dequeued);
                Assert.Equal(1, stats.Enqueued);
                Assert.Equal(0, stats.Queued);
                Assert.Equal(1, stats.Working);
            }
        }
        finally
        {
            await CleanupQueueAsync(queue);
        }
    }

    [Fact]
    public override Task AbandonAsync_WhenRetriesExceeded_MovesToDeadletterAsync()
    {
        return base.AbandonAsync_WhenRetriesExceeded_MovesToDeadletterAsync();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AbandonAsync_WhenRetriesExceededOnExistingQueue_MovesToDeadletterAsync(bool isFifo)
    {
        // Arrange
        string name = isFifo ? GetFifoQueueName() : _queueName;
        using (var creatingQueue = GetNamedQueue(name, retries: 5))
        {
            // A higher retry count gives the queue a redrive maxReceiveCount above this test's retry limit,
            // so the message is dead lettered by SQSQueue rather than by SQS redrive.
            await creatingQueue.EnqueueAsync(new SimpleWorkItem { Data = "dead-letter-test" },
                new QueueEntryOptions { CorrelationId = "correlation-1", GroupId = "tenant-1" });
        }

        var queue = GetNamedQueue(name, retries: 1);

        try
        {
            for (int attempt = 1; attempt <= 2; attempt++)
            {
                var entry = await queue.DequeueAsync(TimeSpan.FromSeconds(5));
                Assert.NotNull(entry);
                await entry.AbandonAsync();
            }

            // Act
            var deadLetteredEntry = await queue.DequeueAsync(TimeSpan.FromSeconds(5));

            // Assert
            Assert.Null(deadLetteredEntry);
            Assert.Null(await queue.DequeueAsync(TimeSpan.FromSeconds(1)));

            string deadLetterName = isFifo ? name[..^".fifo".Length] + "-deadletter.fifo" : name + "-deadletter";
            var deadLetterUrl = await queue.Client.GetQueueUrlAsync(deadLetterName, TestCancellationToken);
            var response = await queue.Client.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = deadLetterUrl.QueueUrl,
                MessageAttributeNames = ["All"],
                MessageSystemAttributeNames = ["All"],
                WaitTimeSeconds = 1
            }, TestCancellationToken);

            var message = Assert.Single(response.Messages);
            Assert.Contains("dead-letter-test", message.Body);
            Assert.Equal("tenant-1", message.Attributes["MessageGroupId"]);
            Assert.Equal("correlation-1", message.MessageAttributes["CorrelationId"].StringValue);
        }
        finally
        {
            await CleanupQueueAsync(queue);
        }
    }

    [Fact]
    public async Task AbandonAsync_WhenRetriesExceededWithoutRedrivePolicy_DeletesMessageAsync()
    {
        // Arrange
        using (var creatingQueue = GetNamedQueue(_queueName, supportDeadLetter: false))
            await creatingQueue.EnqueueAsync(new SimpleWorkItem { Data = "no-redrive" });

        var queue = GetNamedQueue(_queueName, retries: 1);

        try
        {
            for (int attempt = 1; attempt <= 2; attempt++)
            {
                var entry = await queue.DequeueAsync(TimeSpan.FromSeconds(5));
                Assert.NotNull(entry);
                await entry.AbandonAsync();
            }

            // Act
            var deadLetteredEntry = await queue.DequeueAsync(TimeSpan.FromSeconds(5));

            // Assert
            Assert.Null(deadLetteredEntry);
            Assert.Null(await queue.DequeueAsync(TimeSpan.FromSeconds(1)));
            await Assert.ThrowsAsync<QueueDoesNotExistException>(() => queue.Client.GetQueueUrlAsync(_queueName + "-deadletter", TestCancellationToken));
        }
        finally
        {
            await CleanupQueueAsync(queue);
        }
    }

    [Fact]
    public override Task AbandonAsync_WithGroupId_PreservesGroupIdOnRetryAsync()
    {
        return base.AbandonAsync_WithGroupId_PreservesGroupIdOnRetryAsync();
    }

    [Fact]
    public override Task AbandonAsync_WithGroupIdAndRetryDelay_PreservesGroupIdOnRetryAsync()
    {
        return base.AbandonAsync_WithGroupIdAndRetryDelay_PreservesGroupIdOnRetryAsync();
    }

    [Fact]
    public async Task DeleteQueueAsync_WithExternalDeadletterQueue_KeepsDeadletterQueueAsync()
    {
        // Arrange
        string externalDeadLetterName = _queueName + "-external-dlq";
        var queue = GetNamedQueue(_queueName);
        var deadLetter = await queue.Client.CreateQueueAsync(externalDeadLetterName, TestCancellationToken);

        try
        {
            var deadLetterAttributes = await queue.Client.GetQueueAttributesAsync(deadLetter.QueueUrl, [QueueAttributeName.QueueArn], TestCancellationToken);
            await queue.Client.CreateQueueAsync(new CreateQueueRequest
            {
                QueueName = _queueName,
                Attributes = new Dictionary<string, string>
                {
                    [QueueAttributeName.RedrivePolicy] = $$"""{"deadLetterTargetArn":"{{deadLetterAttributes.QueueARN}}","maxReceiveCount":"10"}"""
                }
            }, TestCancellationToken);

            await queue.EnqueueAsync(new SimpleWorkItem { Data = "test" });
            await queue.GetQueueStatsAsync();

            // Act
            await queue.DeleteQueueAsync();

            // Assert
            await Assert.ThrowsAsync<QueueDoesNotExistException>(() => queue.Client.GetQueueUrlAsync(_queueName, TestCancellationToken));
            var externalUrl = await queue.Client.GetQueueUrlAsync(externalDeadLetterName, TestCancellationToken);
            Assert.Equal(deadLetter.QueueUrl, externalUrl.QueueUrl);
        }
        finally
        {
            await queue.Client.DeleteQueueAsync(deadLetter.QueueUrl, TestCancellationToken);
            queue.Dispose();
        }
    }

    [Fact]
    public async Task DequeueAsync_WithSameGroupOnFifoQueue_ReturnsMessagesInOrderAsync()
    {
        // Arrange
        var queue = GetNamedQueue(GetFifoQueueName());
        string?[] expected = ["first", "second", "third"];

        try
        {
            foreach (string? data in expected)
                await queue.EnqueueAsync(new SimpleWorkItem { Data = data }, new QueueEntryOptions { GroupId = "tenant-1" });

            // Act
            var actual = new List<string?>();
            for (int i = 0; i < expected.Length; i++)
            {
                var entry = await queue.DequeueAsync(TimeSpan.FromSeconds(5));
                Assert.NotNull(entry);
                Assert.Equal("tenant-1", entry.GroupId);
                actual.Add(entry.Value.Data);
                await entry.CompleteAsync();
            }

            // Assert
            Assert.Equal(expected, actual);
        }
        finally
        {
            await CleanupQueueAsync(queue);
        }
    }

    [Fact]
    public async Task EnqueueAsync_WithDeliveryDelayOnFifoQueue_ThrowsQueueExceptionAsync()
    {
        // Arrange
        var queue = GetNamedQueue(GetFifoQueueName());
        var options = new QueueEntryOptions { GroupId = "tenant-1", DeliveryDelay = TimeSpan.FromSeconds(5) };

        try
        {
            // Act & Assert
            await Assert.ThrowsAsync<QueueException>(() => queue.EnqueueAsync(new SimpleWorkItem { Data = "test" }, options));
        }
        finally
        {
            await CleanupQueueAsync(queue);
        }
    }

    [Fact]
    public async Task EnqueueAsync_WithDuplicateUniqueIdOnFifoQueue_DeliversOnceAsync()
    {
        // Arrange
        var queue = GetNamedQueue(GetFifoQueueName());

        try
        {
            // Act
            await queue.EnqueueAsync(new SimpleWorkItem { Data = "first" }, new QueueEntryOptions { GroupId = "tenant-1", UniqueId = "order-1" });
            await queue.EnqueueAsync(new SimpleWorkItem { Data = "duplicate" }, new QueueEntryOptions { GroupId = "tenant-1", UniqueId = "order-1" });

            // Assert
            var entry = await queue.DequeueAsync(TimeSpan.FromSeconds(5));
            Assert.NotNull(entry);
            Assert.Equal("first", entry.Value.Data);
            await entry.CompleteAsync();

            Assert.Null(await queue.DequeueAsync(TimeSpan.FromSeconds(2)));
        }
        finally
        {
            await CleanupQueueAsync(queue);
        }
    }

    [Fact]
    public override Task EnqueueAsync_WhenEnqueuingHandlerClearsGroupId_EnqueuesWithoutGroupAsync()
    {
        return base.EnqueueAsync_WhenEnqueuingHandlerClearsGroupId_EnqueuesWithoutGroupAsync();
    }

    [Fact]
    public override Task EnqueueAsync_WithEmptyGroupId_EnqueuesWithoutGroupAsync()
    {
        return base.EnqueueAsync_WithEmptyGroupId_EnqueuesWithoutGroupAsync();
    }

    [Fact]
    public async Task EnqueueAsync_WithFifoQueueName_CreatesFifoDeadletterQueueAsync()
    {
        // Arrange
        string name = GetFifoQueueName();
        string expectedDeadLetterName = name[..^".fifo".Length] + "-deadletter.fifo";
        var queue = GetNamedQueue(name);

        try
        {
            // Act
            await queue.EnqueueAsync(new SimpleWorkItem { Data = "test" }, new QueueEntryOptions { GroupId = "tenant-1" });

            // Assert
            var queueUrl = await queue.Client.GetQueueUrlAsync(name, TestCancellationToken);
            var queueAttributes = await queue.Client.GetQueueAttributesAsync(queueUrl.QueueUrl, [QueueAttributeName.FifoQueue, QueueAttributeName.RedrivePolicy], TestCancellationToken);
            Assert.Equal("true", queueAttributes.Attributes[QueueAttributeName.FifoQueue]);
            Assert.Equal(expectedDeadLetterName, queueAttributes.Attributes.DeadLetterQueue());

            var deadLetterUrl = await queue.Client.GetQueueUrlAsync(expectedDeadLetterName, TestCancellationToken);
            var deadLetterAttributes = await queue.Client.GetQueueAttributesAsync(deadLetterUrl.QueueUrl, [QueueAttributeName.FifoQueue], TestCancellationToken);
            Assert.Equal("true", deadLetterAttributes.Attributes[QueueAttributeName.FifoQueue]);
        }
        finally
        {
            await CleanupQueueAsync(queue);
        }
    }

    [Fact]
    public override Task EnqueueAsync_WithGroupId_RoundTripsGroupIdAsync()
    {
        return base.EnqueueAsync_WithGroupId_RoundTripsGroupIdAsync();
    }

    [Theory]
    [InlineData(128, true)]
    [InlineData(129, false)]
    public async Task EnqueueAsync_WithGroupIdLength_EnforcesSqsMaximumAsync(int length, bool isValid)
    {
        // Arrange
        var queue = GetNamedQueue(_queueName);
        var options = new QueueEntryOptions { GroupId = new string('a', length) };

        try
        {
            // Act
            var exception = await Record.ExceptionAsync(async () => await queue.EnqueueAsync(new SimpleWorkItem { Data = "test" }, options));

            // Assert
            if (isValid)
                Assert.Null(exception);
            else
                Assert.IsType<ArgumentException>(exception);
        }
        finally
        {
            await CleanupQueueAsync(queue);
        }
    }

    [Fact]
    public async Task EnqueueAsync_WithGroupIdOnStandardQueue_SendsMessageGroupIdAsync()
    {
        // Arrange
        var queue = GetNamedQueue(_queueName);

        try
        {
            // Act
            await queue.EnqueueAsync(new SimpleWorkItem { Data = "group-id-test" }, new QueueEntryOptions { GroupId = "tenant-123" });

            // Assert
            var entry = await queue.DequeueAsync(TimeSpan.FromSeconds(5));
            var sqsEntry = Assert.IsType<SQSQueueEntry<SimpleWorkItem>>(entry);
            Assert.Equal("tenant-123", sqsEntry.UnderlyingMessage.Attributes["MessageGroupId"]);
            Assert.Equal("tenant-123", sqsEntry.GroupId);
            await sqsEntry.CompleteAsync();
        }
        finally
        {
            await CleanupQueueAsync(queue);
        }
    }

    [Theory]
    [InlineData("tenant 1")]
    [InlineData("tenant\u00e9")]
    [InlineData("tenant\t1")]
    public async Task EnqueueAsync_WithInvalidGroupId_ThrowsBeforeEnqueuingAsync(string groupId)
    {
        // Arrange
        var queue = GetNamedQueue(_queueName);
        int enqueuingCount = 0;
        using var _ = queue.Enqueuing.AddSyncHandler((_, _) => enqueuingCount++);

        try
        {
            // Act & Assert
            await Assert.ThrowsAsync<ArgumentException>(() => queue.EnqueueAsync(new SimpleWorkItem { Data = "test" }, new QueueEntryOptions { GroupId = groupId }));
            Assert.Equal(0, enqueuingCount);
        }
        finally
        {
            await CleanupQueueAsync(queue);
        }
    }

    [Theory]
    [InlineData("order 1")]
    [InlineData("order\u00e9")]
    public async Task EnqueueAsync_WithInvalidUniqueIdOnFifoQueue_ThrowsArgumentExceptionAsync(string uniqueId)
    {
        // Arrange
        var queue = GetNamedQueue(GetFifoQueueName());
        var options = new QueueEntryOptions { GroupId = "tenant-1", UniqueId = uniqueId };

        try
        {
            // Act & Assert
            await Assert.ThrowsAsync<ArgumentException>(() => queue.EnqueueAsync(new SimpleWorkItem { Data = "test" }, options));
        }
        finally
        {
            await CleanupQueueAsync(queue);
        }
    }

    [Fact]
    public async Task EnqueueAsync_WithMoreThanTenAttributes_ThrowsQueueExceptionAsync()
    {
        // Arrange
        var queue = GetNamedQueue(_queueName);
        int enqueuingCount = 0;
        using var _ = queue.Enqueuing.AddSyncHandler((_, _) => enqueuingCount++);
        var options = new QueueEntryOptions { CorrelationId = "correlation-id" };
        for (int i = 0; i < 10; i++)
            options.Properties["property" + i] = "value" + i;

        try
        {
            // Act & Assert
            await Assert.ThrowsAsync<QueueException>(() => queue.EnqueueAsync(new SimpleWorkItem { Data = "test" }, options));
            Assert.Equal(0, enqueuingCount);
        }
        finally
        {
            await CleanupQueueAsync(queue);
        }
    }

    [Fact]
    public async Task EnqueueAsync_WhenEnqueuingHandlerClearsGroupIdOnFifoQueue_ThrowsQueueExceptionAsync()
    {
        // Arrange
        var queue = GetNamedQueue(GetFifoQueueName());
        using var _ = queue.Enqueuing.AddSyncHandler((_, args) => args.Options.GroupId = null);

        try
        {
            // Act & Assert
            await Assert.ThrowsAsync<QueueException>(() => queue.EnqueueAsync(new SimpleWorkItem { Data = "test" }, new QueueEntryOptions { GroupId = "tenant-1" }));
        }
        finally
        {
            await CleanupQueueAsync(queue);
        }
    }

    [Fact]
    public async Task EnqueueAsync_WhenEnqueuingHandlerSetsInvalidGroupId_ThrowsArgumentExceptionAsync()
    {
        // Arrange
        var queue = GetNamedQueue(_queueName);
        using var _ = queue.Enqueuing.AddSyncHandler((_, args) => args.Options.GroupId = "invalid group");

        try
        {
            // Act & Assert
            await Assert.ThrowsAsync<ArgumentException>(() => queue.EnqueueAsync(new SimpleWorkItem { Data = "test" }));
        }
        finally
        {
            await CleanupQueueAsync(queue);
        }
    }

    [Fact]
    public async Task EnqueueAsync_WithoutGroupIdOnFifoQueue_ThrowsQueueExceptionAsync()
    {
        // Arrange
        var queue = GetNamedQueue(GetFifoQueueName());

        try
        {
            // Act & Assert
            await Assert.ThrowsAsync<QueueException>(() => queue.EnqueueAsync(new SimpleWorkItem { Data = "test" }));
        }
        finally
        {
            await CleanupQueueAsync(queue);
        }
    }

    [Fact]
    public async Task EnqueueAsync_WithoutUniqueIdOnFifoQueue_DeliversIdenticalMessagesAsync()
    {
        // Arrange
        var queue = GetNamedQueue(GetFifoQueueName());

        try
        {
            // Act
            await queue.EnqueueAsync(new SimpleWorkItem { Data = "same" }, new QueueEntryOptions { GroupId = "tenant-1" });
            await queue.EnqueueAsync(new SimpleWorkItem { Data = "same" }, new QueueEntryOptions { GroupId = "tenant-1" });

            // Assert
            for (int i = 0; i < 2; i++)
            {
                var entry = await queue.DequeueAsync(TimeSpan.FromSeconds(5));
                Assert.NotNull(entry);
                Assert.Equal("same", entry.Value.Data);
                await entry.CompleteAsync();
            }
        }
        finally
        {
            await CleanupQueueAsync(queue);
        }
    }

    [Fact]
    public async Task EnqueueAsync_WithPropertyNamedCorrelationId_UsesCorrelationIdOptionAsync()
    {
        // Arrange
        var queue = GetNamedQueue(_queueName);
        var options = new QueueEntryOptions { CorrelationId = "real-correlation-id" };
        options.Properties["CorrelationId"] = "property-value";

        try
        {
            // Act
            string? id = await queue.EnqueueAsync(new SimpleWorkItem { Data = "test" }, options);

            // Assert
            Assert.False(String.IsNullOrEmpty(id));
            var entry = await queue.DequeueAsync(TimeSpan.FromSeconds(5));
            Assert.NotNull(entry);
            Assert.Equal("real-correlation-id", entry.CorrelationId);
            await entry.CompleteAsync();
        }
        finally
        {
            await CleanupQueueAsync(queue);
        }
    }

    [Fact]
    public async Task EnqueueAsync_WithUniqueIdOnStandardQueue_DoesNotSendDeduplicationIdAsync()
    {
        // Arrange
        var queue = GetNamedQueue(_queueName);

        try
        {
            // Act
            string? id = await queue.EnqueueAsync(new SimpleWorkItem { Data = "unique-id-test" }, new QueueEntryOptions { UniqueId = "my-unique-id" });

            // Assert
            Assert.False(String.IsNullOrEmpty(id));
            var entry = await queue.DequeueAsync(TimeSpan.FromSeconds(5));
            var sqsEntry = Assert.IsType<SQSQueueEntry<SimpleWorkItem>>(entry);
            Assert.Equal("unique-id-test", sqsEntry.Value.Data);
            Assert.False(sqsEntry.UnderlyingMessage.Attributes.ContainsKey("MessageDeduplicationId"));
            await sqsEntry.CompleteAsync();
        }
        finally
        {
            await CleanupQueueAsync(queue);
        }
    }

    private static string GetFifoQueueName() => "foundatio-" + Guid.NewGuid().ToString("N").Substring(10) + ".fifo";

    private SQSQueue<SimpleWorkItem> GetNamedQueue(string name, int retries = 1, bool supportDeadLetter = true)
    {
        var queue = new SQSQueue<SimpleWorkItem>(o => o
            .ConnectionString("serviceurl=http://localhost:4566;AccessKey=xxx;SecretKey=xxx")
            .Name(name)
            .Retries(retries)
            .SupportDeadLetter(supportDeadLetter)
            .RetryDelay(_ => TimeSpan.Zero)
            .WorkItemTimeout(TimeSpan.FromMinutes(5))
            .DequeueInterval(TimeSpan.FromSeconds(1))
            .ReadQueueTimeout(TimeSpan.FromSeconds(1))
            .MetricsPollingInterval(TimeSpan.Zero)
            .LoggerFactory(Log));

        _logger.LogDebug("Queue Id: {QueueId}", queue.QueueId);
        return queue;
    }

    protected override async Task CleanupQueueAsync(IQueue<SimpleWorkItem> queue)
    {
        await base.CleanupQueueAsync(queue);
        await Task.Delay(TimeSpan.FromSeconds(2));
    }
}
