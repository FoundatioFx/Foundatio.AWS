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
    /// <summary>
    /// SQS allows at most 10 message attributes per message.
    /// See <see href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html">SQS message metadata</see>.
    /// </summary>
    private const int MaxMessageAttributes = 10;

    /// <summary>
    /// Maximum length of <c>MessageGroupId</c> and <c>MessageDeduplicationId</c>.
    /// See <see href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html">SendMessage</see>.
    /// </summary>
    private const int MaxSqsIdLength = 128;

    /// <summary>
    /// Maximum length of a queue name, including the <c>.fifo</c> suffix.
    /// See <see href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_CreateQueue.html">CreateQueue</see>.
    /// </summary>
    private const int MaxQueueNameLength = 80;

    /// <summary>
    /// Required suffix of FIFO queue names.
    /// See <see href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queues.html">FIFO queues</see>.
    /// </summary>
    private const string FifoQueueSuffix = ".fifo";

    private const string CorrelationIdAttributeName = "CorrelationId";

    /// <summary>
    /// Minimum wait after a failed receive in the worker loop. The SDK already retries transient errors with backoff
    /// (see <see href="https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html">retry behavior</see>),
    /// so errors that reach the worker are usually persistent (for example access denied). This keeps them from becoming
    /// a tight request loop when <c>DequeueInterval</c> is zero.
    /// </summary>
    private static readonly TimeSpan MinDequeueErrorBackoff = TimeSpan.FromSeconds(1);

    private readonly AsyncLock _lock = new();
    private readonly Lazy<AmazonSQSClient> _client;
    private readonly bool _isFifo;
    private string? _queueUrl;
    private DeadLetterQueue? _deadLetterQueue;
    private string? _ownedDeadLetterUrl;

    private long _enqueuedCount;
    private long _dequeuedCount;
    private long _completedCount;
    private long _abandonedCount;
    private long _workerErrorCount;

    public SQSQueue(SQSQueueOptions<T> options) : base(options)
    {
        _isFifo = _options.Name.EndsWith(FifoQueueSuffix, StringComparison.Ordinal);

        // TODO: Flow through the options like retries and the like.
        _client = new Lazy<AmazonSQSClient>(() =>
        {
            var credentials = options.Credentials ?? DefaultAWSCredentialsIdentityResolver.GetCredentials();

            if (String.IsNullOrEmpty(options.ServiceUrl))
            {
                var region = options.Region ?? FallbackRegionFactory.GetRegionEndpoint();
                return new AmazonSQSClient(credentials, region);
            }

            return new AmazonSQSClient(
                credentials,
                new AmazonSQSConfig
                {
                    RegionEndpoint = RegionEndpoint.USEast1,
                    ServiceURL = options.ServiceUrl
                });
        });
    }

    public SQSQueue(Builder<SQSQueueOptionsBuilder<T>, SQSQueueOptions<T>> builder)
        : this(builder(new SQSQueueOptionsBuilder<T>()).Build()) { }

    public AmazonSQSClient Client => _client.Value;

    protected override bool SupportsGroupId => true;


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

        // Enqueuing handlers may change the options, so the options that are actually sent are validated again.
        ValidateEntryOptions(options);

        var message = new SendMessageRequest
        {
            QueueUrl = _queueUrl,
            MessageBody = _serializer.SerializeToString(data),
            MessageAttributes = CreateMessageAttributes(options)
        };

        // SQS only accepts a deduplication id on FIFO queues and requires one unless ContentBasedDeduplication is enabled.
        if (_isFifo)
            message.MessageDeduplicationId = !String.IsNullOrEmpty(options.UniqueId) ? options.UniqueId : Guid.NewGuid().ToString("N");

        if (!String.IsNullOrEmpty(options.GroupId))
            message.MessageGroupId = options.GroupId;

        // NOTE: Any delay defined here will override any delay configured in the SQS.
        if (options.DeliveryDelay.HasValue && !_isFifo)
        {
            int delaySeconds = (int)options.DeliveryDelay.Value.TotalSeconds;
            message.DelaySeconds = Math.Max(0, Math.Min(900, delaySeconds));
        }


        var response = await _client.Value.SendMessageAsync(message).AnyContext();
        if (response.HttpStatusCode is not System.Net.HttpStatusCode.OK)
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
        while ((response?.Messages is null || response.Messages.Count is 0) && !linkedCancellationToken.IsCancellationRequested)
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

        if (response?.Messages is null || response.Messages.Count is 0)
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

        // get attributes from dead letter
        string? deadLetterUrl = await ResolveDeadLetterUrlAsync(queueAttributes.Attributes).AnyContext();
        if (deadLetterUrl is not null)
        {
            var deadRequest = new GetQueueAttributesRequest(deadLetterUrl, attributeNames);
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
        if (_ownedDeadLetterUrl is not null)
        {
            await _client.Value.DeleteQueueAsync(_ownedDeadLetterUrl).AnyContext();
        }

        _deadLetterQueue = null;
        _ownedDeadLetterUrl = null;

        _enqueuedCount = 0;
        _dequeuedCount = 0;
        _completedCount = 0;
        _abandonedCount = 0;
        _workerErrorCount = 0;
    }

    protected override void StartWorkingImpl(Func<IQueueEntry<T>, CancellationToken, Task> handler, bool autoComplete, CancellationToken cancellationToken)
    {
        if (handler is null)
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
                    var errorBackoff = _options.DequeueInterval > MinDequeueErrorBackoff ? _options.DequeueInterval : MinDequeueErrorBackoff;
                    await _timeProvider.SafeDelay(errorBackoff, linkedCancellationTokenSource.Token).AnyContext();
                }

                if (linkedCancellationTokenSource.IsCancellationRequested || entry is null)
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
                            _logger.LogError(abandonEx, "Worker error abandoning queue entry {QueueEntryId}; it will be redelivered after its visibility timeout: {Message}", entry.Id, abandonEx.Message);
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
        string deadLetterName = GetDeadLetterQueueName();
        if (_options.SupportDeadLetter && deadLetterName.Length > MaxQueueNameLength)
            throw new QueueException($"The dead letter queue name {deadLetterName} exceeds the SQS limit of {MaxQueueNameLength} characters; use a queue name of at most {MaxQueueNameLength - (deadLetterName.Length - _options.Name.Length)} characters.");

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
            QueueName = deadLetterName,
            Attributes = GetQueueTypeAttributes()
        };
        var createDeadResponse = await _client.Value.CreateQueueAsync(createDeadRequest).AnyContext();
        _deadLetterQueue = new DeadLetterQueue(deadLetterName, createDeadResponse.QueueUrl);
        _ownedDeadLetterUrl = createDeadResponse.QueueUrl;

        // step 3, get dead letter attributes
        var attributeNames = new List<string> { QueueAttributeName.QueueArn };
        var deadAttributeRequest = new GetQueueAttributesRequest(createDeadResponse.QueueUrl, attributeNames);
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
        if (_isFifo)
            attributes[QueueAttributeName.FifoQueue] = "true";

        return attributes;
    }

    /// <summary>
    /// SQS requires FIFO queue names (including a FIFO queue's dead letter queue) to end with <c>.fifo</c>.
    /// </summary>
    private string GetDeadLetterQueueName()
    {
        return _isFifo
            ? _options.Name[..^FifoQueueSuffix.Length] + "-deadletter" + FifoQueueSuffix
            : _options.Name + "-deadletter";
    }

    /// <summary>
    /// Resolves the dead letter queue from the source queue's current redrive policy, so a policy that is added, changed or
    /// removed while this instance is running is honored. The URL is cached per dead letter queue name, so an unchanged
    /// policy costs no <c>GetQueueUrl</c> call. Lookup failures propagate so exhausted messages are retained.
    /// Only a dead letter queue that follows the <c>{name}-deadletter</c> convention is deleted by <see cref="QueueBase{T,TOptions}.DeleteQueueAsync"/>.
    /// </summary>
    private async Task<string?> ResolveDeadLetterUrlAsync(IDictionary<string, string>? queueAttributes = null)
    {
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
                return null;

            var cached = _deadLetterQueue;
            if (cached is not null && String.Equals(cached.Name, deadLetterName, StringComparison.Ordinal))
                return cached.Url;

            var deadResponse = await _client.Value.GetQueueUrlAsync(deadLetterName).AnyContext();
            if (String.IsNullOrEmpty(deadResponse.QueueUrl))
                throw new QueueException($"Unable to resolve dead letter queue {deadLetterName} for {_options.Name}.");

            _deadLetterQueue = new DeadLetterQueue(deadLetterName, deadResponse.QueueUrl);
            if (String.Equals(deadLetterName, GetDeadLetterQueueName(), StringComparison.Ordinal))
                _ownedDeadLetterUrl = deadResponse.QueueUrl;

            return deadResponse.QueueUrl;
        }
        catch (AmazonServiceException ex)
        {
            _logger.LogWarning(ex, "Unable to resolve dead letter queue {DeadLetterQueueName} for {QueueName}: {Message}", deadLetterName, _options.Name, ex.Message);
            throw;
        }
    }

    /// <summary>
    /// Validates the options against SQS limits. The SQS emulators don't enforce these limits, and checking them here fails
    /// before <see cref="QueueBase{T,TOptions}.Enqueuing"/> handlers such as duplicate detection reserve state. It runs again
    /// after the handlers because they may change the options. The checks don't allocate.
    /// </summary>
    private void ValidateEntryOptions(QueueEntryOptions options)
    {
        ValidateSqsId(options.GroupId, nameof(QueueEntryOptions.GroupId));

        int attributeCount = CountMessageAttributes(options);
        if (attributeCount > MaxMessageAttributes)
            throw new QueueException($"SQS allows at most {MaxMessageAttributes} message attributes per message (including CorrelationId and TraceState), but {attributeCount} were provided.");

        if (!_isFifo)
            return;

        if (String.IsNullOrEmpty(options.GroupId))
            throw new QueueException($"A GroupId is required when enqueuing to the FIFO queue {_options.Name}.");

        if (options.DeliveryDelay > TimeSpan.Zero)
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
    /// Counts the attributes <see cref="CreateMessageAttributes"/> would send. SQS attribute names are case-sensitive, so a
    /// property only replaces <c>CorrelationId</c> when its name matches exactly.
    /// </summary>
    private static int CountMessageAttributes(QueueEntryOptions options)
    {
        int count = options.Properties.Count;
        if (!String.IsNullOrEmpty(options.CorrelationId) && !HasCorrelationIdProperty(options.Properties))
            count++;

        return count;
    }

    private static bool HasCorrelationIdProperty(IDictionary<string, string> properties)
    {
        if (!properties.ContainsKey(CorrelationIdAttributeName))
            return false;

        foreach (string key in properties.Keys)
        {
            if (String.Equals(key, CorrelationIdAttributeName, StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Builds the SQS message attributes (custom properties plus <c>CorrelationId</c>), or <c>null</c> when there are none.
    /// <see cref="ValidateEntryOptions"/> enforces the attribute limit.
    /// </summary>
    private static Dictionary<string, MessageAttributeValue>? CreateMessageAttributes(QueueEntryOptions options)
    {
        bool hasCorrelationId = !String.IsNullOrEmpty(options.CorrelationId);
        if (options.Properties.Count is 0 && !hasCorrelationId)
            return null;

        var attributes = new Dictionary<string, MessageAttributeValue>(options.Properties.Count + 1, StringComparer.Ordinal);
        foreach (var property in options.Properties)
            attributes[property.Key] = new MessageAttributeValue { DataType = "String", StringValue = property.Value };

        if (hasCorrelationId)
            attributes[CorrelationIdAttributeName] = new MessageAttributeValue { DataType = "String", StringValue = options.CorrelationId };

        return attributes;
    }
    private async Task DeadLetterMessageAsync(SQSQueueEntry<T> entry)
    {
        _logger.LogInformation("Exceeded retry limit ({Attempts}/{Retries}), moving message {QueueEntryId} to dead letter", entry.Attempts, _options.Retries, entry.Id);

        string? deadLetterUrl = _options.SupportDeadLetter ? await ResolveDeadLetterUrlAsync().AnyContext() : null;
        if (deadLetterUrl is not null)
        {
            var message = entry.UnderlyingMessage;
            var deadMessage = new SendMessageRequest
            {
                QueueUrl = deadLetterUrl,
                MessageBody = message.Body,
                // The received attribute dictionary is owned by this entry, which is deleted below, so it is reused rather than copied.
                MessageAttributes = message.MessageAttributes is { Count: > 0 } ? message.MessageAttributes : null,
                MessageGroupId = message.Attributes.MessageGroupId(),
                MessageDeduplicationId = _isFifo ? message.MessageId : null
            };

            await _client.Value.SendMessageAsync(deadMessage).AnyContext();
        }

        await _client.Value.DeleteMessageAsync(_queueUrl, entry.UnderlyingMessage.ReceiptHandle).AnyContext();
    }

    private sealed record DeadLetterQueue(string Name, string Url);

    private static SQSQueueEntry<T> ToQueueEntry(IQueueEntry<T> entry)
    {
        if (entry is not SQSQueueEntry<T> result)
            throw new ArgumentException($"Expected {nameof(SQSQueueEntry<T>)} but received unknown queue entry type {entry.GetType()}");

        return result;
    }
}
