using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using Amazon.S3.Model;
using Foundatio.Storage;

namespace Foundatio.AWS.Extensions;

internal static class Extensions
{
    internal static bool IsSuccessful(this HttpStatusCode code)
    {
        return (int)code < 400;
    }

    internal static FileSpec ToFileInfo(this S3Object blob)
    {
        if (blob == null)
            return null;

        return new FileSpec
        {
            Path = blob.Key,
            Size = blob.Size.GetValueOrDefault(),
            Created = blob.LastModified?.ToUniversalTime() ?? DateTime.MinValue, // TODO: Need to fix this
            Modified = blob.LastModified?.ToUniversalTime() ?? DateTime.MinValue,
        };
    }

    internal static IEnumerable<S3Object> MatchesPattern(this IEnumerable<S3Object> blobs, Regex patternRegex)
    {
        return blobs.Where(blob =>
        {
            var info = blob.ToFileInfo();
            if (info?.Path is null)
                return false;

            return patternRegex == null || patternRegex.IsMatch(info.Path);
        });
    }

    internal static bool IsDirectory(this FileSpec file)
    {
        return file.Path is not null && file.Size is 0 && file.Path.EndsWith("/");
    }
}
