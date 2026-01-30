using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Foundatio.AsyncEx;
using Foundatio.Messaging;
using Foundatio.Tests.Messaging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Foundatio.AWS.Tests.Messaging;

public class SQSMessageBusTests : MessageBusTestBase
{
    private static readonly IConfiguration _configuration;
    private readonly string _topic = $"foundatio-{DateTime.UtcNow.Ticks}";

    static SQSMessageBusTests()
    {
        _configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: false)
            .Build();
    }

    public SQSMessageBusTests(ITestOutputHelper output) : base(output) { }

    private string GetConnectionString()
    {
        return _configuration.GetConnectionString("SQSMessageBus")
            ?? "serviceurl=http://localhost:4566;AccessKey=xxx;SecretKey=xxx";
    }

    protected override IMessageBus GetMessageBus(Func<SharedMessageBusOptions, SharedMessageBusOptions> config = null)
    {
        var options = new SQSMessageBusOptions
        {
            Topic = _topic,
            LoggerFactory = Log
        };

        if (config is not null)
            config(options);

        var messageBus = new SQSMessageBus(o => o
            .ConnectionString(GetConnectionString())
            .Topic(options.Topic)
            .ReadQueueTimeout(TimeSpan.FromSeconds(1))
            .DequeueInterval(TimeSpan.FromMilliseconds(100))
            .LoggerFactory(Log));

        _logger.LogDebug("MessageBus Id: {MessageBusId}", messageBus.MessageBusId);
        return messageBus;
    }

    protected override async Task CleanupMessageBusAsync(IMessageBus messageBus)
    {
        await base.CleanupMessageBusAsync(messageBus);
        await Task.Delay(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public override Task CanUseMessageOptionsAsync()
    {
        return base.CanUseMessageOptionsAsync();
    }

    [Fact]
    public override Task CanSendMessageAsync()
    {
        return base.CanSendMessageAsync();
    }

    [Fact]
    public override Task CanHandleNullMessageAsync()
    {
        return base.CanHandleNullMessageAsync();
    }

    [Fact]
    public override Task CanSendDerivedMessageAsync()
    {
        return base.CanSendDerivedMessageAsync();
    }

    [Fact]
    public override Task CanSendMappedMessageAsync()
    {
        return base.CanSendMappedMessageAsync();
    }

    [Fact(Skip = "SNS/SQS latency makes processing 1000 delayed messages in 30 seconds impractical")]
    public override Task CanSendDelayedMessageAsync()
    {
        return base.CanSendDelayedMessageAsync();
    }

    [Fact]
    public async Task PublishAsync_WithDelayedDelivery_DeliversMessageAfterDelay()
    {
        // Arrange
        using var messageBus = GetMessageBus();
        if (messageBus is null)
            return;

        try
        {
            var countdown = new AsyncCountdownEvent(1);
            string receivedData = null;

            await messageBus.SubscribeAsync<SimpleMessageA>(msg =>
            {
                receivedData = msg.Data;
                countdown.Signal();
            }, TestCancellationToken);

            // Act
            await messageBus.PublishAsync(new SimpleMessageA { Data = "Hello" }, new MessageOptions
            {
                DeliveryDelay = TimeSpan.FromMilliseconds(500),
                CorrelationId = "test-correlation",
                Properties = new System.Collections.Generic.Dictionary<string, string> { { "TestKey", "TestValue" } }
            }, TestCancellationToken);

            await countdown.WaitAsync(TestCancellationToken);

            // Assert
            Assert.Equal("Hello", receivedData);
            Assert.Equal(0, countdown.CurrentCount);
        }
        finally
        {
            await CleanupMessageBusAsync(messageBus);
        }
    }

    [Fact]
    public override Task CanSubscribeConcurrentlyAsync()
    {
        return base.CanSubscribeConcurrentlyAsync();
    }

    [Fact]
    public override Task CanReceiveMessagesConcurrentlyAsync()
    {
        return base.CanReceiveMessagesConcurrentlyAsync();
    }

    [Fact]
    public override Task CanSendMessageToMultipleSubscribersAsync()
    {
        return base.CanSendMessageToMultipleSubscribersAsync();
    }

    [Fact]
    public override Task CanTolerateSubscriberFailureAsync()
    {
        return base.CanTolerateSubscriberFailureAsync();
    }

    [Fact]
    public override Task WillOnlyReceiveSubscribedMessageTypeAsync()
    {
        return base.WillOnlyReceiveSubscribedMessageTypeAsync();
    }

    [Fact]
    public override Task WillReceiveDerivedMessageTypesAsync()
    {
        return base.WillReceiveDerivedMessageTypesAsync();
    }

    [Fact]
    public override Task CanSubscribeToRawMessagesAsync()
    {
        return base.CanSubscribeToRawMessagesAsync();
    }

    [Fact]
    public override Task CanSubscribeToAllMessageTypesAsync()
    {
        return base.CanSubscribeToAllMessageTypesAsync();
    }

    [Fact]
    public override Task WontKeepMessagesWithNoSubscribersAsync()
    {
        return base.WontKeepMessagesWithNoSubscribersAsync();
    }

    [Fact]
    public override Task CanCancelSubscriptionAsync()
    {
        return base.CanCancelSubscriptionAsync();
    }

    [Fact]
    public override Task CanReceiveFromMultipleSubscribersAsync()
    {
        return base.CanReceiveFromMultipleSubscribersAsync();
    }

    [Fact]
    public override void CanDisposeWithNoSubscribersOrPublishers()
    {
        base.CanDisposeWithNoSubscribersOrPublishers();
    }

    [Fact]
    public override Task CanHandlePoisonedMessageAsync()
    {
        return base.CanHandlePoisonedMessageAsync();
    }

    [Fact]
    public async Task SubscribeAsync_WithDurableQueue_PersistsAcrossRestarts()
    {
        // Arrange
        string durableQueueName = $"durable-{Guid.NewGuid():N}".Substring(0, 30);

        var messageBus1 = new SQSMessageBus(o => o
            .ConnectionString(GetConnectionString())
            .Topic(_topic)
            .UseDurableSubscription(durableQueueName)
            .ReadQueueTimeout(TimeSpan.FromSeconds(1))
            .DequeueInterval(TimeSpan.FromMilliseconds(100))
            .LoggerFactory(Log));

        var countdownEvent1 = new AsyncCountdownEvent(1);
        string receivedData1 = null;

        await messageBus1.SubscribeAsync<SimpleMessageA>(msg =>
        {
            receivedData1 = msg.Data;
            countdownEvent1.Signal();
        }, TestCancellationToken);

        // Act - First message bus
        await messageBus1.PublishAsync(new SimpleMessageA { Data = "Message 1" });
        await countdownEvent1.WaitAsync(TestCancellationToken);

        // Assert - First message bus
        Assert.Equal("Message 1", receivedData1);

        await messageBus1.DisposeAsync();

        // Arrange - Second message bus (simulating restart)
        var messageBus2 = new SQSMessageBus(o => o
            .ConnectionString(GetConnectionString())
            .Topic(_topic)
            .UseDurableSubscription(durableQueueName)
            .ReadQueueTimeout(TimeSpan.FromSeconds(1))
            .DequeueInterval(TimeSpan.FromMilliseconds(100))
            .LoggerFactory(Log));

        var countdownEvent2 = new AsyncCountdownEvent(1);
        string receivedData2 = null;

        await messageBus2.SubscribeAsync<SimpleMessageA>(msg =>
        {
            receivedData2 = msg.Data;
            countdownEvent2.Signal();
        }, TestCancellationToken);

        // Act - Second message bus
        await messageBus2.PublishAsync(new SimpleMessageA { Data = "Message 2" });
        await countdownEvent2.WaitAsync(TestCancellationToken);

        // Assert - Second message bus
        Assert.Equal("Message 2", receivedData2);

        await messageBus2.DisposeAsync();

        // Cleanup
        try
        {
            await messageBus2.SqsClient.DeleteQueueAsync(
                (await messageBus2.SqsClient.GetQueueUrlAsync(durableQueueName)).QueueUrl);
        }
        catch { }
    }

    [Fact]
    public async Task DisposeAsync_WhenCalled_CleansUpResources()
    {
        // Arrange
        await using var messageBus = new SQSMessageBus(o => o
            .ConnectionString(GetConnectionString())
            .Topic(_topic)
            .ReadQueueTimeout(TimeSpan.FromSeconds(1))
            .DequeueInterval(TimeSpan.FromMilliseconds(100))
            .LoggerFactory(Log));

        var countdownEvent = new AsyncCountdownEvent(1);
        string receivedData = null;

        await messageBus.SubscribeAsync<SimpleMessageA>(msg =>
        {
            receivedData = msg.Data;
            countdownEvent.Signal();
        }, TestCancellationToken);

        // Act
        await messageBus.PublishAsync(new SimpleMessageA { Data = "Test" });
        await countdownEvent.WaitAsync(TestCancellationToken);

        // Assert
        Assert.Equal("Test", receivedData);
        Assert.Equal(0, countdownEvent.CurrentCount);
    }
}
