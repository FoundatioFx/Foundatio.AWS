using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using Foundatio.AWS.Extensions;
using Foundatio.Extensions;
using Foundatio.Serializer;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Foundatio.Storage
{
    public class S3FileStorage : IFileStorage
    {
        private readonly string _bucket;
        private readonly ISerializer _serializer;
        private readonly AmazonS3Client _client;
        private readonly bool _useChunkEncoding;
        private readonly S3CannedACL _cannedAcl;
        private readonly ILogger _logger;

        public S3FileStorage(S3FileStorageOptions options)
        {
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            _serializer = options.Serializer ?? DefaultSerializer.Instance;
            _logger = options.LoggerFactory?.CreateLogger(GetType()) ?? NullLogger.Instance;

            _bucket = options.Bucket;
            _useChunkEncoding = options.UseChunkEncoding ?? true;
            _cannedAcl = options.CannedACL;

            var credentials = options.Credentials ?? FallbackCredentialsFactory.GetCredentials();

            if (String.IsNullOrEmpty(options.ServiceUrl))
            {
                var region = options.Region ?? FallbackRegionFactory.GetRegionEndpoint();
                _client = new AmazonS3Client(credentials, region);
            }
            else
            {
                _client = new AmazonS3Client(credentials, new AmazonS3Config
                {
                    RegionEndpoint = RegionEndpoint.USEast1,
                    ServiceURL = options.ServiceUrl,
                    ForcePathStyle = true,
                    HttpClientFactory = options.HttpClientFactory
                });
            }
        }

        public S3FileStorage(Builder<S3FileStorageOptionsBuilder, S3FileStorageOptions> builder)
            : this(builder(new S3FileStorageOptionsBuilder()).Build()) { }

        ISerializer IHaveSerializer.Serializer => _serializer;

        public AmazonS3Client Client => _client;
        public string Bucket => _bucket;
        public S3CannedACL CannedACL => _cannedAcl;

        [Obsolete($"Use {nameof(GetFileStreamAsync)} with {nameof(FileAccess)} instead to define read or write behaviour of stream")]
        public Task<Stream> GetFileStreamAsync(string path, CancellationToken cancellationToken = default)
            => GetFileStreamAsync(path, StreamMode.Read, cancellationToken);

        public async Task<Stream> GetFileStreamAsync(string path, StreamMode streamMode, CancellationToken cancellationToken = default)
        {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            if (streamMode is StreamMode.Write)
                throw new NotSupportedException($"Stream mode {streamMode} is not supported.");

            var req = new GetObjectRequest
            {
                BucketName = _bucket,
                Key = NormalizePath(path)
            };

            _logger.LogTrace("Getting file stream for {Path}", req.Key);

            var response = await _client.GetObjectAsync(req, cancellationToken).AnyContext();
            if (!response.HttpStatusCode.IsSuccessful())
            {
                _logger.LogError("[{HttpStatusCode}] Unable to get file stream for {Path}", response.HttpStatusCode, req.Key);
                return null;
            }

            return new ActionableStream(response.ResponseStream, () =>
            {
                _logger.LogTrace("Disposing file stream for {Path}", req.Key);
                response.Dispose();
            });
        }

        public async Task<FileSpec> GetFileInfoAsync(string path)
        {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            var req = new GetObjectMetadataRequest
            {
                BucketName = _bucket,
                Key = NormalizePath(path)
            };

            _logger.LogTrace("Getting file info for {Path}", req.Key);

            try
            {
                var response = await _client.GetObjectMetadataAsync(req).AnyContext();
                if (!response.HttpStatusCode.IsSuccessful())
                {
                    _logger.LogDebug("[{HttpStatusCode}] Unable to get file info for {Path}", response.HttpStatusCode, req.Key);
                    return null;
                }

                return new FileSpec
                {
                    Path = req.Key,
                    Size = response.ContentLength,
                    Created = response.LastModified.ToUniversalTime(),  // TODO: Need to fix this
                    Modified = response.LastModified.ToUniversalTime()
                };
            }
            catch (AmazonS3Exception ex)
            {
                _logger.LogError(ex, "Unable to get file info for {Path}: {Message}", req.Key, ex.Message);
                return null;
            }
        }

        public async Task<bool> ExistsAsync(string path)
        {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            var req = new GetObjectMetadataRequest
            {
                BucketName = _bucket,
                Key = NormalizePath(path)
            };

            _logger.LogTrace("Getting file info for {Path}", req.Key);

            try
            {
                var response = await _client.GetObjectMetadataAsync(req).AnyContext();
                if (response.HttpStatusCode.IsSuccessful())
                    return true;

                if (response.HttpStatusCode == HttpStatusCode.NotFound)
                    return false;

                if (!response.HttpStatusCode.IsSuccessful())
                {
                    _logger.LogDebug("[{HttpStatusCode}] Unable to get file info for {Path}", response.HttpStatusCode, req.Key);
                    return false;
                }
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode is HttpStatusCode.NotFound)
            {
                return false;
            }
            catch (AmazonS3Exception ex)
            {
                _logger.LogError(ex, "Unable to get file info for {Path}: {Message}", req.Key, ex.Message);
            }

            return false;
        }

        public async Task<bool> SaveFileAsync(string path, Stream stream, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));
            if (stream == null)
                throw new ArgumentNullException(nameof(stream));

            var req = new PutObjectRequest
            {
                CannedACL = _cannedAcl,
                BucketName = _bucket,
                Key = NormalizePath(path),
                AutoResetStreamPosition = false,
                AutoCloseStream = !stream.CanSeek,
                InputStream = stream.CanSeek ? stream : AmazonS3Util.MakeStreamSeekable(stream),
                UseChunkEncoding = _useChunkEncoding
            };

            _logger.LogTrace("Saving {Path}", req.Key);
            var response = await _client.PutObjectAsync(req, cancellationToken).AnyContext();
            return response.HttpStatusCode.IsSuccessful();
        }

        public async Task<bool> RenameFileAsync(string path, string newPath, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));
            if (String.IsNullOrEmpty(newPath))
                throw new ArgumentNullException(nameof(newPath));

            var request = new CopyObjectRequest
            {
                CannedACL = _cannedAcl,
                SourceBucket = _bucket,
                SourceKey = NormalizePath(path),
                DestinationBucket = _bucket,
                DestinationKey = NormalizePath(newPath)
            };

            _logger.LogInformation("Renaming {Path} to {NewPath}", request.SourceKey, request.DestinationKey);
            var response = await _client.CopyObjectAsync(request, cancellationToken).AnyContext();
            if (!response.HttpStatusCode.IsSuccessful())
            {
                _logger.LogError("[{HttpStatusCode}] Unable to rename {Path} to {NewPath}", response.HttpStatusCode, request.SourceKey, request.DestinationKey);
                return false;
            }

            var deleteRequest = new DeleteObjectRequest
            {
                BucketName = _bucket,
                Key = NormalizePath(path)
            };

            _logger.LogDebug("Deleting renamed {Path}", deleteRequest.Key);
            var deleteResponse = await _client.DeleteObjectAsync(deleteRequest, cancellationToken).AnyContext();
            if (!deleteResponse.HttpStatusCode.IsSuccessful())
            {
                _logger.LogError("[{HttpStatusCode}] Unable to delete renamed {Path}", deleteResponse.HttpStatusCode, deleteRequest.Key);
                return false;
            }

            return true;
        }

        public async Task<bool> CopyFileAsync(string path, string targetPath, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));
            if (String.IsNullOrEmpty(targetPath))
                throw new ArgumentNullException(nameof(targetPath));

            var request = new CopyObjectRequest
            {
                CannedACL = _cannedAcl,
                SourceBucket = _bucket,
                SourceKey = NormalizePath(path),
                DestinationBucket = _bucket,
                DestinationKey = NormalizePath(targetPath)
            };

            _logger.LogInformation("Copying {Path} to {TargetPath}", request.SourceKey, request.DestinationKey);
            var response = await _client.CopyObjectAsync(request, cancellationToken).AnyContext();
            return response.HttpStatusCode.IsSuccessful();
        }

        public async Task<bool> DeleteFileAsync(string path, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (String.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            var request = new DeleteObjectRequest
            {
                BucketName = _bucket,
                Key = NormalizePath(path)
            };

            _logger.LogTrace("Deleting {Path}", request.Key);
            var response = await _client.DeleteObjectAsync(request, cancellationToken).AnyContext();
            return response.HttpStatusCode.IsSuccessful();
        }

        public async Task<int> DeleteFilesAsync(string searchPattern = null, CancellationToken cancellationToken = new CancellationToken())
        {
            var criteria = GetRequestCriteria(searchPattern);
            int count = 0;
            const int PAGE_SIZE = 100;

            var listRequest = new ListObjectsV2Request { BucketName = _bucket, Prefix = criteria.Prefix, MaxKeys = PAGE_SIZE };
            var deleteRequest = new DeleteObjectsRequest { BucketName = _bucket };
            var errors = new List<DeleteError>();

            ListObjectsV2Response listResponse;
            do
            {
                listResponse = await _client.ListObjectsV2Async(listRequest, cancellationToken).AnyContext();
                listRequest.ContinuationToken = listResponse.NextContinuationToken;

                var keys = listResponse.S3Objects.MatchesPattern(criteria.Pattern).Select(o => new KeyVersion { Key = o.Key }).ToArray();
                if (keys.Length == 0)
                    continue;

                deleteRequest.Objects.AddRange(keys);

                _logger.LogInformation("Deleting {FileCount} files matching {SearchPattern}", keys.Length, searchPattern);
                var deleteResponse = await _client.DeleteObjectsAsync(deleteRequest, cancellationToken).AnyContext();
                if (deleteResponse.DeleteErrors.Count > 0)
                {
                    // retry 1 time, continue on.
                    var deleteRetryRequest = new DeleteObjectsRequest { BucketName = _bucket };
                    deleteRetryRequest.Objects.AddRange(deleteResponse.DeleteErrors.Select(e => new KeyVersion { Key = e.Key }));
                    var deleteRetryResponse = await _client.DeleteObjectsAsync(deleteRetryRequest, cancellationToken).AnyContext();
                    if (deleteRetryResponse.DeleteErrors.Count > 0)
                        errors.AddRange(deleteRetryResponse.DeleteErrors);
                }

                _logger.LogTrace("Deleted {FileCount} files matching {SearchPattern}", deleteResponse.DeletedObjects.Count, searchPattern);
                count += deleteResponse.DeletedObjects.Count;
                deleteRequest.Objects.Clear();
            } while (listResponse.IsTruncated && !cancellationToken.IsCancellationRequested);

            if (errors.Count > 0)
            {
                int more = errors.Count > 20 ? errors.Count - 20 : 0;
                throw new Exception($"Unable to delete all S3 entries \"{String.Join(",", errors.Take(20).Select(e => e.Key))}\"{(more > 0 ? $" plus {more} more" : "")}.");
            }

            _logger.LogTrace("Finished deleting {FileCount} files matching {SearchPattern}", count, searchPattern);
            return count;
        }

        public async Task<PagedFileListResult> GetPagedFileListAsync(int pageSize = 100, string searchPattern = null, CancellationToken cancellationToken = default)
        {
            if (pageSize <= 0)
                return PagedFileListResult.Empty;

            var criteria = GetRequestCriteria(searchPattern);
            var result = new PagedFileListResult(_ => GetFiles(criteria, pageSize, cancellationToken));
            await result.NextPageAsync().AnyContext();

            return result;
        }

        private async Task<NextPageResult> GetFiles(SearchCriteria criteria, int pageSize, CancellationToken cancellationToken, string continuationToken = null)
        {
            var req = new ListObjectsV2Request
            {
                BucketName = _bucket,
                MaxKeys = pageSize,
                Prefix = criteria.Prefix,
                ContinuationToken = continuationToken
            };

            _logger.LogTrace(
                s => s.Property("Limit", req.MaxKeys),
                "Getting file list matching {Prefix} and {Pattern}...", criteria.Prefix, criteria.Pattern
            );

            var response = await _client.ListObjectsV2Async(req, cancellationToken).AnyContext();
            return new NextPageResult
            {
                Success = response.HttpStatusCode.IsSuccessful(),
                HasMore = response.IsTruncated,
                Files = response.S3Objects.MatchesPattern(criteria.Pattern).Select(blob => blob.ToFileInfo()).ToList(),
                NextPageFunc = response.IsTruncated ? _ => GetFiles(criteria, pageSize, cancellationToken, response.NextContinuationToken) : null
            };
        }

        private string NormalizePath(string path)
        {
            return path?.Replace('\\', '/');
        }

        private class SearchCriteria
        {
            public string Prefix { get; set; }
            public Regex Pattern { get; set; }
        }

        private SearchCriteria GetRequestCriteria(string searchPattern)
        {
            if (String.IsNullOrEmpty(searchPattern))
                return new SearchCriteria { Prefix = String.Empty };

            string normalizedSearchPattern = NormalizePath(searchPattern);
            int wildcardPos = normalizedSearchPattern.IndexOf('*');
            bool hasWildcard = wildcardPos >= 0;

            string prefix = normalizedSearchPattern;
            Regex patternRegex = null;

            if (hasWildcard)
            {
                patternRegex = new Regex($"^{Regex.Escape(normalizedSearchPattern).Replace("\\*", ".*?")}$");
                int slashPos = normalizedSearchPattern.LastIndexOf('/');
                prefix = slashPos >= 0 ? normalizedSearchPattern.Substring(0, slashPos) : String.Empty;
            }

            return new SearchCriteria
            {
                Prefix = prefix,
                Pattern = patternRegex
            };
        }

        public void Dispose()
        {
            _client?.Dispose();
        }
    }
}
