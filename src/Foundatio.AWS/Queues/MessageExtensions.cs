﻿using System;
using Amazon.SQS.Model;

namespace Foundatio.Queues;

public static class MessageExtensions
{
    public static int ApproximateReceiveCount(this Message message)
    {
        if (message?.Attributes == null)
            return 0;

        return message.Attributes.ApproximateReceiveCount();
    }

    public static DateTime SentTimestamp(this Message message)
    {
        if (message?.Attributes == null)
            return DateTime.MinValue;

        return message.Attributes.SentTimestamp();
    }

    public static string CorrelationId(this Message message)
    {
        if (message?.Attributes == null)
            return null;

        return message.MessageAttributes.CorrelationId();
    }
}
