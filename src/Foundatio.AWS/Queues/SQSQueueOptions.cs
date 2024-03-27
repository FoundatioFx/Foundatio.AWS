﻿using System;
using Amazon;
using Amazon.Runtime;

namespace Foundatio.Queues;

public class SQSQueueOptions<T> : SharedQueueOptions<T> where T : class
{
    public string ConnectionString { get; set; }
    public AWSCredentials Credentials { get; set; }
    public RegionEndpoint Region { get; set; }
    public string ServiceUrl { get; set; }
    public bool CanCreateQueue { get; set; } = true;
    public bool SupportDeadLetter { get; set; } = true;
    public TimeSpan ReadQueueTimeout { get; set; } = TimeSpan.FromSeconds(20);
    public TimeSpan DequeueInterval { get; set; } = TimeSpan.FromSeconds(1);
    public string KmsMasterKeyId { get; set; }
    public int KmsDataKeyReusePeriodSeconds { get; set; }
    public bool SqsManagedSseEnabled { get; set; } = false;


    private static readonly Random _random = new();
    public Func<int, TimeSpan> RetryDelay { get; set; } = attempt =>
    {
        return TimeSpan.FromSeconds(Math.Pow(2, attempt)) + TimeSpan.FromMilliseconds(_random.Next(0, 100));
    };
}

public class SQSQueueOptionsBuilder<T> : SharedQueueOptionsBuilder<T, SQSQueueOptions<T>, SQSQueueOptionsBuilder<T>> where T : class
{
    public SQSQueueOptionsBuilder<T> ConnectionString(string connectionString)
    {
        if (string.IsNullOrEmpty(connectionString))
            throw new ArgumentNullException(nameof(connectionString));
        Target.ConnectionString = connectionString;
        return this;
    }

    public SQSQueueOptionsBuilder<T> ReadQueueTimeout(TimeSpan timeout)
    {
        if (timeout < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(timeout), "Read Queue timeout must be greater than or equal to zero.");

        Target.ReadQueueTimeout = timeout;
        return this;
    }

    public SQSQueueOptionsBuilder<T> RetryDelay(Func<int, TimeSpan> retryDelay)
    {
        Target.RetryDelay = retryDelay;
        return this;
    }

    public SQSQueueOptionsBuilder<T> DequeueInterval(TimeSpan interval)
    {
        if (interval < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(interval), "Dequeue interval must be greater than or equal to zero.");

        Target.DequeueInterval = interval;
        return this;
    }

    public SQSQueueOptionsBuilder<T> CanCreateQueue(bool enabled)
    {
        Target.CanCreateQueue = enabled;
        return this;
    }

    public SQSQueueOptionsBuilder<T> EnableCreateQueue() => CanCreateQueue(true);

    public SQSQueueOptionsBuilder<T> DisableCreateQueue() => CanCreateQueue(false);

    public SQSQueueOptionsBuilder<T> SupportDeadLetter(bool supported)
    {
        Target.SupportDeadLetter = supported;
        return this;
    }

    public SQSQueueOptionsBuilder<T> EnableDeadLetter() => SupportDeadLetter(true);

    public SQSQueueOptionsBuilder<T> DisableDeadLetter() => SupportDeadLetter(false);

    public SQSQueueOptionsBuilder<T> Credentials(AWSCredentials credentials)
    {
        if (credentials == null)
            throw new ArgumentNullException(nameof(credentials));
        Target.Credentials = credentials;
        return this;
    }

    public SQSQueueOptionsBuilder<T> Credentials(string accessKey, string secretKey)
    {
        if (String.IsNullOrEmpty(accessKey))
            throw new ArgumentNullException(nameof(accessKey));
        if (String.IsNullOrEmpty(secretKey))
            throw new ArgumentNullException(nameof(secretKey));

        Target.Credentials = new BasicAWSCredentials(accessKey, secretKey);
        return this;
    }

    public SQSQueueOptionsBuilder<T> Region(RegionEndpoint region)
    {
        if (region == null)
            throw new ArgumentNullException(nameof(region));
        Target.Region = region;
        return this;
    }

    public SQSQueueOptionsBuilder<T> Region(string region)
    {
        if (String.IsNullOrEmpty(region))
            throw new ArgumentNullException(nameof(region));
        Target.Region = RegionEndpoint.GetBySystemName(region);
        return this;
    }

    public SQSQueueOptionsBuilder<T> UseKmsEncryption(string kmsMasterKeyId, int kmsKeyReusePeriodSeconds = 300)
    {
        if (String.IsNullOrEmpty(kmsMasterKeyId))
            throw new ArgumentNullException(nameof(kmsMasterKeyId));
        Target.KmsMasterKeyId = kmsMasterKeyId;
        Target.KmsDataKeyReusePeriodSeconds = kmsKeyReusePeriodSeconds; // Default kms key reuse period is 300 seconds
        Target.SqsManagedSseEnabled = false; // Must set Sqs Managed SSE to false, as it is either KMS or Sqs Managed - can't have both
        return this;
    }

    public SQSQueueOptionsBuilder<T> UseSqsManagedEncryption()
    {
        Target.SqsManagedSseEnabled = true;
        Target.KmsMasterKeyId = null; // Must set KmsMasterKeyId to null, as it is either KMS or Sqs Managed - can't have both
        return this;
    }

    public override SQSQueueOptions<T> Build()
    {
        if (String.IsNullOrEmpty(Target.ConnectionString))
            return Target;

        var connectionString = new SQSQueueConnectionStringBuilder(Target.ConnectionString);
        if (Target.Credentials == null)
            Target.Credentials = connectionString.GetCredentials();

        if (Target.Region == null)
            Target.Region = connectionString.GetRegion();

        if (String.IsNullOrEmpty(Target.ServiceUrl) && !String.IsNullOrEmpty(connectionString.ServiceUrl))
            Target.ServiceUrl = connectionString.ServiceUrl;

        return Target;
    }
}
