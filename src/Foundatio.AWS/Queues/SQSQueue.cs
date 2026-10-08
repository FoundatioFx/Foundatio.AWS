using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.Credentials;
using Amazon.SQS;
using Amazon.SQS.Model;
using Foundatio.AsyncEx;
using Foundatio.Extensions;
using Foundatio.Serializer;
using Microsoft.Extensions.Logging;

namespace Foundatio.Queues;

public class SQSQueue<T> : QueueBase<T, SQSQueueOptions<T>> where T : class
{
    private const int MaxMessageAttributes = 10;
    private const int MaxSqsIdLength = 128;
    private const string FifoQueueSuffix = ".fifo";

    private readonly AsyncLock _lock = new();
    private readonly Lazy<AmazonSQSClient> _client;
    private string? _queueUrl;
    private string? _deadUrl;
    private bool _ownsDeadLetterQueue;

    private long _enqueuedCount;
    private long _dequeuedCount;
    private long _completedCount;
    private long _abandonedCount;
    private long _workerErrorCount;

    public SQSQueue(SQSQueueOptions<T> options) : base(options)
    {
        // TODO: Flow through the options like retries and the like.
        _client = new Lazy<AmazonSQSClient>(() =>
        {
            var credentials = options.Credentials ?? DefaultAWSCredentialsIdentityResolver.GetCredentials();

            if (String.IsNullOrEmpty(options.ServiceUrl))
            {
                var region = options.Region ?? FallbackRegionFactory.GetRegionEndpoint();
                return new AmazonSQSClient(credentials, new AmazonSQSConfig
                {
                    RegionEndpoint = region,
                    HttpClientFactory = options.HttpClientFactory
                });
            }

            return new AmazonSQSClient(
                credentials,
                new AmazonSQSConfig
                {
                    RegionEndpoint = RegionEndpoint.USEast1,
                    ServiceURL = options.ServiceUrl,
                    HttpClientFactory = options.HttpClientFactory
                });
        });
    }

    public SQSQueue(Builder<SQSQueueOptionsBuilder<T>, SQSQueueOptions<T>> builder)
        : this(builder(new SQSQueueOptionsBuilder<T>()).Build()) { }

    public AmazonSQSClient Client => _client.Value;

    protected override bool SupportsGroupId => true;

    private bool IsFifo => _options.Name.EndsWith(FifoQueueSuffix, StringComparison.Ordinal);

    protected override async Task EnsureQueueCreatedAsync(CancellationToken cancellationToken = default)
    {
        if (!String.IsNullOrEmpty(_queueUrl))
            return;

        using (await _lock.LockAsync(cancellationToken).AnyContext())
        {
            if (!String.IsNullOrEmpty(_queueUrl))
                return;

            try
            {
                var urlResponse = await _client.Value.GetQueueUrlAsync(_options.Name, cancellationToken).AnyContext();
                _queueUrl = urlResponse.QueueUrl;
            }
            catch (QueueDoesNotExistException)
            {
                if (!_options.CanCreateQueue)
                    throw;
            }

            if (!String.IsNullOrEmpty(_queueUrl))
                return;

            await CreateQueueAsync().AnyContext();
        }
    }

    protected override async Task<string?> EnqueueImplAsync(T data, QueueEntryOptions options)
    {
        ValidateEntryOptions(options);

        if (!await OnEnqueuingAsync(data, options).AnyContext())
            return null;

        ValidateEntryOptions(options);

        var message = new SendMessageRequest
        {
            QueueUrl = _queueUrl,
            MessageBody = _serializer.SerializeToString(data)
        };

        // SQS only accepts a deduplication id on FIFO queues and requires one unless ContentBasedDeduplication is enabled.
        if (IsFifo)
            message.MessageDeduplicationId = !String.IsNullOrEmpty(options.UniqueId) ? options.UniqueId : Guid.NewGuid().ToString("N");

        if (!String.IsNullOrEmpty(options.GroupId))
            message.MessageGroupId = options.GroupId;

        // NOTE: Any delay defined here will override any delay configured in the SQS.
        if (options.DeliveryDelay.HasValue)
        {
            int delaySeconds = (int)options.DeliveryDelay.Value.TotalSeconds;
            message.DelaySeconds = Math.Max(0, Math.Min(900, delaySeconds));
        }

        var attributes = CreateMessageAttributes(options);
        if (attributes.Count > 0)
            message.MessageAttributes = attributes;

        var response = await _client.Value.SendMessageAsync(message).AnyContext();
        if (response.HttpStatusCode != System.Net.HttpStatusCode.OK)
            throw new QueueException("Failed to send SQS message.");

        _logger.LogTrace("Enqueued SQS message {MessageId} GroupId={GroupId}", response.MessageId, options.GroupId);

        Interlocked.Increment(ref _enqueuedCount);
        var entry = new QueueEntry<T>(response.MessageId, options.CorrelationId, data, this, _timeProvider.GetUtcNow().UtcDateTime, 0) { GroupId = options.GroupId };
        await OnEnqueuedAsync(entry).AnyContext();

        return response.MessageId;
    }

    protected override async Task<IQueueEntry<T>?> DequeueImplAsync(CancellationToken linkedCancellationToken)
    {
        // sqs doesn't support already canceled token, change timeout and token for sqs pattern
        int visibilityTimeout = (int)Math.Round(_options.WorkItemTimeout.TotalSeconds, MidpointRounding.AwayFromZero);
        int waitTimeout = linkedCancellationToken.IsCancellationRequested ? 0 : (int)Math.Round(_options.ReadQueueTimeout.TotalSeconds, MidpointRounding.AwayFromZero);

        var request = new ReceiveMessageRequest
        {
            QueueUrl = _queueUrl,
            MaxNumberOfMessages = 1,
            VisibilityTimeout = visibilityTimeout,
            WaitTimeSeconds = waitTimeout,
            MessageSystemAttributeNames = ["All"],
            MessageAttributeNames = ["All"]
        };

        // receive message local function
        Task<ReceiveMessageResponse> ReceiveMessageAsync()
        {
            _logger.LogTrace("Checking for SQS message... IsCancellationRequested={IsCancellationRequested} VisibilityTimeout={VisibilityTimeout} WaitTimeSeconds={WaitTimeSeconds}", linkedCancellationToken.IsCancellationRequested, visibilityTimeout, waitTimeout);

            // The aws sdk will not abort a http long pull operation when the cancellation token is cancelled.
            // The aws sdk will throw the OperationCanceledException after the long poll http call is returned and
            // the message will be marked as in-flight but not returned from this call: https://github.com/aws/aws-sdk-net/issues/1680
            return _client.Value.ReceiveMessageAsync(request, CancellationToken.None);
        }

        var response = await ReceiveMessageAsync().AnyContext();

        // retry loop
        while ((response?.Messages is null || response.Messages.Count == 0) && !linkedCancellationToken.IsCancellationRequested)
        {
            if (_options.DequeueInterval > TimeSpan.Zero)
            {
                try
                {
                    await _timeProvider.Delay(_options.DequeueInterval, linkedCancellationToken).AnyContext();
                }
                catch (OperationCanceledException)
                {
                    _logger.LogTrace("Operation cancelled while waiting to retry dequeue");
                }
            }

            response = await ReceiveMessageAsync().AnyContext();
        }

        if (response?.Messages is null || response.Messages.Count == 0)
        {
            _logger.LogTrace("Response null or 0 message count");
            return null;
        }

        Interlocked.Increment(ref _dequeuedCount);

        _logger.LogTrace("Received message {MessageId} IsCancellationRequested={IsCancellationRequested}", response.Messages[0].MessageId, linkedCancellationToken.IsCancellationRequested);

        var message = response.Messages[0];
        string body = message.Body;

        T? data;
        Exception? deserializeException = null;
        try
        {
            data = _serializer.Deserialize<T>(body);
        }
        catch (Exception ex)
        {
            data = null;
            deserializeException = ex;
        }

        if (data is null)
        {
            _logger.LogWarning(deserializeException, "Error deserializing message {MessageId} (receive count {ReceiveCount}), abandoning for retry",
                message.MessageId, message.ApproximateReceiveCount());

            // Poison message: null! is intentional — deserialization failed, entry is immediately abandoned.
            var poisonEntry = new SQSQueueEntry<T>(message, null!, this);
            await AbandonAsync(poisonEntry).AnyContext();
            return null;
        }

        var entry = new SQSQueueEntry<T>(message, data, this);

        if (entry.Attempts > _options.Retries + 1)
        {
            await DeadLetterMessageAsync(entry).AnyContext();
            Interlocked.Increment(ref _abandonedCount);
            return null;
        }

        await OnDequeuedAsync(entry).AnyContext();

        return entry;
    }

    public override async Task RenewLockAsync(IQueueEntry<T> queueEntry)
    {
        _logger.LogDebug("Queue {QueueName} renew lock item: {QueueEntryId}", _options.Name, queueEntry.Id);

        var entry = ToQueueEntry(queueEntry);
        int visibilityTimeout = (int)Math.Round(_options.WorkItemTimeout.TotalSeconds, MidpointRounding.AwayFromZero);
        var request = new ChangeMessageVisibilityRequest
        {
            QueueUrl = _queueUrl,
            VisibilityTimeout = visibilityTimeout,
            ReceiptHandle = entry.UnderlyingMessage.ReceiptHandle
        };

        await _client.Value.ChangeMessageVisibilityAsync(request).AnyContext();
        await OnLockRenewedAsync(entry).AnyContext();

        _logger.LogTrace("Renew lock done: {QueueEntryId} MessageId={MessageId} VisibilityTimeout={VisibilityTimeout}", queueEntry.Id, entry.UnderlyingMessage.MessageId, visibilityTimeout);
    }

    public override async Task CompleteAsync(IQueueEntry<T> queueEntry)
    {
        _logger.LogDebug("Queue {QueueName} complete item: {QueueEntryId}", _options.Name, queueEntry.Id);
        if (queueEntry.IsAbandoned || queueEntry.IsCompleted)
            throw new QueueException("Queue entry has already been completed or abandoned.");

        var entry = ToQueueEntry(queueEntry);
        var request = new DeleteMessageRequest
        {
            QueueUrl = _queueUrl,
            ReceiptHandle = entry.UnderlyingMessage.ReceiptHandle,
        };

        await _client.Value.DeleteMessageAsync(request).AnyContext();

        Interlocked.Increment(ref _completedCount);
        queueEntry.MarkCompleted();
        await OnCompletedAsync(queueEntry).AnyContext();
        _logger.LogTrace("Complete done: {QueueEntryId}", queueEntry.Id);
    }

    public override async Task AbandonAsync(IQueueEntry<T> entry)
    {
        _logger.LogDebug("Queue {QueueName} ({QueueId}) abandon item: {QueueEntryId}", _options.Name, QueueId, entry.Id);

        if (entry.IsAbandoned || entry.IsCompleted)
            throw new QueueException("Queue entry has already been completed or abandoned.");

        var sqsQueueEntry = ToQueueEntry(entry);

        if (sqsQueueEntry.Attempts > _options.Retries)
        {
            await DeadLetterMessageAsync(sqsQueueEntry).AnyContext();
        }
        else
        {
            int visibilityTimeout = (int)Math.Round(_options.RetryDelay(sqsQueueEntry.Attempts).TotalSeconds, MidpointRounding.AwayFromZero);

            var request = new ChangeMessageVisibilityRequest
            {
                QueueUrl = _queueUrl,
                VisibilityTimeout = visibilityTimeout,
                ReceiptHandle = sqsQueueEntry.UnderlyingMessage.ReceiptHandle,
            };

            await _client.Value.ChangeMessageVisibilityAsync(request).AnyContext();
            _logger.LogTrace("Abandoned queue entry: {QueueEntryId} MessageId={MessageId} VisibilityTimeout={VisibilityTimeout}", sqsQueueEntry.Id, sqsQueueEntry.UnderlyingMessage.MessageId, visibilityTimeout);
        }

        Interlocked.Increment(ref _abandonedCount);
        entry.MarkAbandoned();

        await OnAbandonedAsync(sqsQueueEntry).AnyContext();
        _logger.LogTrace("Abandon complete: {QueueEntryId}", entry.Id);
    }

    protected override Task<IEnumerable<T>> GetDeadletterItemsImplAsync(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    protected override async Task<QueueStats> GetQueueStatsImplAsync()
    {
        if (String.IsNullOrEmpty(_queueUrl))
            return new QueueStats
            {
                Queued = 0,
                Working = 0,
                Deadletter = 0,
                Enqueued = _enqueuedCount,
                Dequeued = _dequeuedCount,
                Completed = _completedCount,
                Abandoned = _abandonedCount,
                Errors = _workerErrorCount,
                Timeouts = 0
            };

        var attributeNames = new List<string> { QueueAttributeName.All };
        var queueRequest = new GetQueueAttributesRequest(_queueUrl, attributeNames);
        var queueAttributes = await _client.Value.GetQueueAttributesAsync(queueRequest).AnyContext();

        int queueCount = queueAttributes.ApproximateNumberOfMessages;
        int workingCount = queueAttributes.ApproximateNumberOfMessagesNotVisible;
        int deadCount = 0;

        // dead letter supported
        if (!_options.SupportDeadLetter)
        {
            return new QueueStats
            {
                Queued = queueCount,
                Working = workingCount,
                Deadletter = deadCount,
                Enqueued = _enqueuedCount,
                Dequeued = _dequeuedCount,
                Completed = _completedCount,
                Abandoned = _abandonedCount,
                Errors = _workerErrorCount,
                Timeouts = 0
            };
        }

        await EnsureDeadLetterUrlAsync(queueAttributes.Attributes).AnyContext();

        // get attributes from dead letter
        if (!String.IsNullOrEmpty(_deadUrl))
        {
            var deadRequest = new GetQueueAttributesRequest(_deadUrl, attributeNames);
            var deadAttributes = await _client.Value.GetQueueAttributesAsync(deadRequest).AnyContext();
            deadCount = deadAttributes.ApproximateNumberOfMessages;
        }

        return new QueueStats
        {
            Queued = queueCount,
            Working = workingCount,
            Deadletter = deadCount,
            Enqueued = _enqueuedCount,
            Dequeued = _dequeuedCount,
            Completed = _completedCount,
            Abandoned = _abandonedCount,
            Errors = _workerErrorCount,
            Timeouts = 0
        };
    }

    protected override async Task DeleteQueueImplAsync()
    {
        if (!String.IsNullOrEmpty(_queueUrl))
        {
            await _client.Value.DeleteQueueAsync(_queueUrl).AnyContext();
        }
        if (!String.IsNullOrEmpty(_deadUrl) && _ownsDeadLetterQueue)
        {
            await _client.Value.DeleteQueueAsync(_deadUrl).AnyContext();
        }

        _enqueuedCount = 0;
        _dequeuedCount = 0;
        _completedCount = 0;
        _abandonedCount = 0;
        _workerErrorCount = 0;
    }

    protected override void StartWorkingImpl(Func<IQueueEntry<T>, CancellationToken, Task> handler, bool autoComplete, CancellationToken cancellationToken)
    {
        if (handler == null)
            throw new ArgumentNullException(nameof(handler));

        var linkedCancellationTokenSource = GetLinkedDisposableCancellationTokenSource(cancellationToken);

        Task.Run(async () =>
        {
            _logger.LogTrace("WorkerLoop Start {QueueName}", _options.Name);

            while (!linkedCancellationTokenSource.IsCancellationRequested)
            {
                _logger.LogTrace("WorkerLoop Signaled {QueueName}", _options.Name);

                IQueueEntry<T>? entry = null;
                try
                {
                    entry = await DequeueImplAsync(linkedCancellationTokenSource.Token).AnyContext();
                }
                catch (OperationCanceledException) { }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref _workerErrorCount);
                    _logger.LogError(ex, "Error on Dequeue: {Message}", ex.Message);
                    try
                    {
                        await _timeProvider.Delay(_options.DequeueInterval, linkedCancellationTokenSource.Token).AnyContext();
                    }
                    catch (OperationCanceledException) { }
                }

                if (linkedCancellationTokenSource.IsCancellationRequested || entry == null)
                    continue;

                try
                {
                    await handler(entry, linkedCancellationTokenSource.Token).AnyContext();
                    if (autoComplete && !entry.IsAbandoned && !entry.IsCompleted && !linkedCancellationTokenSource.IsCancellationRequested)
                        await entry.CompleteAsync().AnyContext();
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref _workerErrorCount);
                    _logger.LogError(ex, "Worker error: {Message}", ex.Message);

                    if (!entry.IsAbandoned && !entry.IsCompleted && !linkedCancellationTokenSource.IsCancellationRequested)
                    {
                        try
                        {
                            await entry.AbandonAsync().AnyContext();
                        }
                        catch (Exception abandonEx)
                        {
                            _logger.LogError(abandonEx, "Worker error abandoning queue entry: {Message}", abandonEx.Message);
                        }
                    }
                }
            }

            _logger.LogTrace("Worker exiting: {QueueName} IsCancellationRequested={IsCancellationRequested}", _options.Name, linkedCancellationTokenSource.IsCancellationRequested);
        }, linkedCancellationTokenSource.Token).ContinueWith(_ => linkedCancellationTokenSource.Dispose());
    }

    public override void Dispose()
    {
        if (!SignalDispose())
        {
            _logger.LogTrace("Queue {QueueName} ({QueueId}) dispose was already called", _options.Name, QueueId);
            return;
        }

        if (_client.IsValueCreated)
            _client.Value.Dispose();

        base.Dispose();
    }

    protected virtual async Task CreateQueueAsync()
    {
        // step 1, create queue
        var createQueueRequest = new CreateQueueRequest
        {
            QueueName = _options.Name,
            Attributes = GetQueueTypeAttributes()
        };

        if (_options.SqsManagedSseEnabled)
        {
            createQueueRequest.Attributes[QueueAttributeName.SqsManagedSseEnabled] = "true";
        }
        else if (!String.IsNullOrEmpty(_options.KmsMasterKeyId))
        {
            createQueueRequest.Attributes[QueueAttributeName.KmsMasterKeyId] = _options.KmsMasterKeyId;
            createQueueRequest.Attributes[QueueAttributeName.KmsDataKeyReusePeriodSeconds] = _options.KmsDataKeyReusePeriodSeconds.ToString();
        }

        var createQueueResponse = await _client.Value.CreateQueueAsync(createQueueRequest).AnyContext();
        _queueUrl = createQueueResponse.QueueUrl;

        if (!_options.SupportDeadLetter)
            return;

        // step 2, create dead letter queue
        var createDeadRequest = new CreateQueueRequest
        {
            QueueName = GetDeadLetterQueueName(),
            Attributes = GetQueueTypeAttributes()
        };
        var createDeadResponse = await _client.Value.CreateQueueAsync(createDeadRequest).AnyContext();
        _deadUrl = createDeadResponse.QueueUrl;
        _ownsDeadLetterQueue = true;

        // step 3, get dead letter attributes
        var attributeNames = new List<string> { QueueAttributeName.QueueArn };
        var deadAttributeRequest = new GetQueueAttributesRequest(_deadUrl, attributeNames);
        var deadAttributeResponse = await _client.Value.GetQueueAttributesAsync(deadAttributeRequest).AnyContext();

        int maxReceiveCount = Math.Max(_options.Retries + 1, 1);
        // step 4, set retry policy
        var redrivePolicy = new JsonObject
        {
            ["maxReceiveCount"] = maxReceiveCount.ToString(),
            ["deadLetterTargetArn"] = deadAttributeResponse.QueueARN
        };

        var attributes = new Dictionary<string, string>
        {
            [QueueAttributeName.RedrivePolicy] = redrivePolicy.ToJsonString()
        };

        var setAttributeRequest = new SetQueueAttributesRequest(_queueUrl, attributes);
        await _client.Value.SetQueueAttributesAsync(setAttributeRequest).AnyContext();
    }

    private Dictionary<string, string> GetQueueTypeAttributes()
    {
        var attributes = new Dictionary<string, string>();
        if (IsFifo)
            attributes[QueueAttributeName.FifoQueue] = "true";

        return attributes;
    }

    /// <summary>
    /// SQS requires FIFO queue names (including a FIFO queue's dead letter queue) to end with <c>.fifo</c>.
    /// </summary>
    private string GetDeadLetterQueueName()
    {
        return IsFifo
            ? _options.Name[..^FifoQueueSuffix.Length] + "-deadletter" + FifoQueueSuffix
            : _options.Name + "-deadletter";
    }

    /// <summary>
    /// Resolves the dead letter queue from the source queue's redrive policy when this instance did not create the queue.
    /// A missing redrive policy is checked again on the next call. Lookup failures propagate so exhausted messages are retained.
    /// Only a dead letter queue that follows the <c>{name}-deadletter</c> convention is deleted by <see cref="QueueBase{T,TOptions}.DeleteQueueAsync"/>.
    /// </summary>
    private async Task EnsureDeadLetterUrlAsync(IDictionary<string, string>? queueAttributes = null)
    {
        if (!String.IsNullOrEmpty(_deadUrl))
            return;

        string? deadLetterName = null;
        try
        {
            if (queueAttributes is null)
            {
                var attributeNames = new List<string> { QueueAttributeName.RedrivePolicy };
                var response = await _client.Value.GetQueueAttributesAsync(new GetQueueAttributesRequest(_queueUrl, attributeNames)).AnyContext();
                queueAttributes = response.Attributes;
            }

            deadLetterName = queueAttributes.DeadLetterQueue();
            if (String.IsNullOrEmpty(deadLetterName))
                return;

            var deadResponse = await _client.Value.GetQueueUrlAsync(deadLetterName).AnyContext();
            if (String.IsNullOrEmpty(deadResponse.QueueUrl))
                throw new QueueException($"Unable to resolve dead letter queue {deadLetterName} for {_options.Name}.");

            _ownsDeadLetterQueue = String.Equals(deadLetterName, GetDeadLetterQueueName(), StringComparison.Ordinal);
            _deadUrl = deadResponse.QueueUrl;
        }
        catch (AmazonServiceException ex)
        {
            _logger.LogWarning(ex, "Unable to resolve dead letter queue {DeadLetterQueueName} for {QueueName}: {Message}", deadLetterName, _options.Name, ex.Message);
            throw;
        }
    }

    /// <summary>
    /// Validates the options against SQS limits. Runs before <see cref="QueueBase{T,TOptions}.Enqueuing"/> handlers so known
    /// option violations are rejected before behaviors such as duplicate detection reserve state, and again after them
    /// because handlers may change the options.
    /// </summary>
    private void ValidateEntryOptions(QueueEntryOptions options)
    {
        ValidateSqsId(options.GroupId, nameof(QueueEntryOptions.GroupId));
        CreateMessageAttributes(options);

        if (!IsFifo)
            return;

        if (String.IsNullOrEmpty(options.GroupId))
            throw new QueueException($"A GroupId is required when enqueuing to the FIFO queue {_options.Name}.");

        if (options.DeliveryDelay.HasValue)
            throw new QueueException($"FIFO queue {_options.Name} does not support per-message delivery delays; configure DelaySeconds on the queue instead.");

        if (!String.IsNullOrEmpty(options.UniqueId))
            ValidateSqsId(options.UniqueId, nameof(QueueEntryOptions.UniqueId));
    }

    /// <summary>
    /// <c>MessageGroupId</c> and <c>MessageDeduplicationId</c> share the same SQS rules: 1-128 printable ASCII characters without spaces.
    /// </summary>
    private static void ValidateSqsId(string? value, string paramName)
    {
        if (value is null)
            return;

        if (value.Length is 0 or > MaxSqsIdLength)
            throw new ArgumentException($"{paramName} must be between 1 and {MaxSqsIdLength} characters for SQS.", paramName);

        foreach (char c in value)
        {
            if (c is < '!' or > '~')
                throw new ArgumentException($"{paramName} may only contain printable ASCII characters ('!' through '~') without spaces for SQS.", paramName);
        }
    }

    /// <summary>
    /// Builds the SQS message attributes (custom properties plus <c>CorrelationId</c>) and enforces the SQS limit of
    /// <see cref="MaxMessageAttributes"/> attributes per message. SQS attribute names are case-sensitive.
    /// </summary>
    private static Dictionary<string, MessageAttributeValue> CreateMessageAttributes(QueueEntryOptions options)
    {
        var attributes = new Dictionary<string, MessageAttributeValue>(StringComparer.Ordinal);
        foreach (var property in options.Properties)
            attributes[property.Key] = new MessageAttributeValue { DataType = "String", StringValue = property.Value };

        if (!String.IsNullOrEmpty(options.CorrelationId))
            attributes["CorrelationId"] = new MessageAttributeValue { DataType = "String", StringValue = options.CorrelationId };

        if (attributes.Count > MaxMessageAttributes)
            throw new QueueException($"SQS allows at most {MaxMessageAttributes} message attributes per message (including CorrelationId and TraceState), but {attributes.Count} were provided.");

        return attributes;
    }

    private async Task DeadLetterMessageAsync(SQSQueueEntry<T> entry)
    {
        _logger.LogInformation("Exceeded retry limit ({Attempts}/{Retries}), moving message {QueueEntryId} to dead letter", entry.Attempts, _options.Retries, entry.Id);

        if (_options.SupportDeadLetter)
            await EnsureDeadLetterUrlAsync().AnyContext();

        if (_options.SupportDeadLetter && !String.IsNullOrEmpty(_deadUrl))
        {
            var deadMessage = new SendMessageRequest
            {
                QueueUrl = _deadUrl,
                MessageBody = entry.UnderlyingMessage.Body
            };

            if (entry.UnderlyingMessage.MessageAttributes is { Count: > 0 })
                deadMessage.MessageAttributes = new Dictionary<string, MessageAttributeValue>(entry.UnderlyingMessage.MessageAttributes);

            string? groupId = entry.UnderlyingMessage.Attributes.MessageGroupId();
            if (!String.IsNullOrEmpty(groupId))
                deadMessage.MessageGroupId = groupId;

            if (IsFifo)
                deadMessage.MessageDeduplicationId = entry.UnderlyingMessage.MessageId;

            await _client.Value.SendMessageAsync(deadMessage).AnyContext();
        }

        await _client.Value.DeleteMessageAsync(_queueUrl, entry.UnderlyingMessage.ReceiptHandle).AnyContext();
    }

    private static SQSQueueEntry<T> ToQueueEntry(IQueueEntry<T> entry)
    {
        if (entry is not SQSQueueEntry<T> result)
            throw new ArgumentException($"Expected {nameof(SQSQueueEntry<T>)} but received unknown queue entry type {entry.GetType()}");

        return result;
    }
}
