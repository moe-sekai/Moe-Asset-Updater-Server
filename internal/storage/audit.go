package storage

import (
	"context"
	"path/filepath"
	"strings"
	"time"

	"moe-asset-server/internal/config"
	"moe-asset-server/internal/protocol"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// AuditS3 scans S3 objects in the given region's bucket and returns objects
// matching the filter criteria (extensions, time window, size range).
func (u *Uploader) AuditS3(ctx context.Context, req protocol.AuditS3Request) (protocol.AuditS3Response, error) {
	var modifiedAfter, modifiedBefore time.Time
	if req.ModifiedAfter != "" {
		t, err := time.Parse(time.RFC3339, req.ModifiedAfter)
		if err != nil {
			return protocol.AuditS3Response{}, err
		}
		modifiedAfter = t
	}
	if req.ModifiedBefore != "" {
		t, err := time.Parse(time.RFC3339, req.ModifiedBefore)
		if err != nil {
			return protocol.AuditS3Response{}, err
		}
		modifiedBefore = t
	}

	extSet := make(map[string]bool, len(req.Extensions))
	for _, ext := range req.Extensions {
		e := strings.ToLower(ext)
		if !strings.HasPrefix(e, ".") {
			e = "." + e
		}
		extSet[e] = true
	}

	limit := req.Limit
	if limit <= 0 {
		limit = 10000
	}

	var result protocol.AuditS3Response
	for _, provider := range u.cfg.Storage.Providers {
		if provider.Kind != "" && !strings.EqualFold(provider.Kind, "s3") {
			continue
		}
		objects, err := auditProvider(ctx, provider, req.Region, extSet, modifiedAfter, modifiedBefore, req.MinSizeBytes, req.MaxSizeBytes, req.PrefixFilter, limit)
		if err != nil {
			return protocol.AuditS3Response{}, err
		}
		result.Objects = append(result.Objects, objects...)
	}
	result.Total = len(result.Objects)
	return result, nil
}

func auditProvider(ctx context.Context, provider config.StorageProviderConfig, region protocol.Region, extSet map[string]bool, after, before time.Time, minSize, maxSize int64, prefixFilter string, limit int) ([]protocol.AuditS3Object, error) {
	client := S3Client(provider)
	bucket := ResolveBucket(provider, region)
	if bucket == "" {
		return nil, nil
	}

	prefix := strings.ReplaceAll(provider.Prefix, "{region}", string(region))
	prefix = strings.ReplaceAll(prefix, "{server}", string(region))
	prefix = strings.Trim(prefix, "/")
	if prefixFilter != "" {
		if prefix != "" {
			prefix = prefix + "/" + strings.TrimPrefix(prefixFilter, "/")
		} else {
			prefix = strings.TrimPrefix(prefixFilter, "/")
		}
	}

	var listPrefix *string
	if prefix != "" {
		p := prefix + "/"
		listPrefix = &p
	}

	var objects []protocol.AuditS3Object
	var continuationToken *string

	for {
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			ContinuationToken: continuationToken,
			MaxKeys:           aws.Int32(1000),
		}
		if listPrefix != nil {
			input.Prefix = listPrefix
		}

		resp, err := client.ListObjectsV2(ctx, input)
		if err != nil {
			return nil, err
		}

		for _, obj := range resp.Contents {
			if obj.Key == nil || obj.LastModified == nil {
				continue
			}
			key := *obj.Key
			ext := strings.ToLower(filepath.Ext(key))

			// Extension filter
			if len(extSet) > 0 && !extSet[ext] {
				continue
			}
			// Time window filter
			if !after.IsZero() && obj.LastModified.Before(after) {
				continue
			}
			if !before.IsZero() && obj.LastModified.After(before) {
				continue
			}
			// Size filter
			size := int64(0)
			if obj.Size != nil {
				size = *obj.Size
			}
			if minSize > 0 && size < minSize {
				continue
			}
			if maxSize > 0 && size > maxSize {
				continue
			}

			// Reverse-resolve bundle path from S3 key.
			suspected := reverseBundlePath(provider, region, key)

			objects = append(objects, protocol.AuditS3Object{
				Key:             key,
				Size:            size,
				LastModified:    *obj.LastModified,
				SuspectedBundle: suspected,
			})

			if len(objects) >= limit {
				return objects, nil
			}
		}

		if resp.IsTruncated == nil || !*resp.IsTruncated {
			break
		}
		continuationToken = resp.NextContinuationToken
	}

	return objects, nil
}

// DeleteS3Objects deletes the specified keys from the region's S3 bucket.
func (u *Uploader) DeleteS3Objects(ctx context.Context, region protocol.Region, keys []string) (int, error) {
	deleted := 0
	for _, provider := range u.cfg.Storage.Providers {
		if provider.Kind != "" && !strings.EqualFold(provider.Kind, "s3") {
			continue
		}
		client := S3Client(provider)
		bucket := ResolveBucket(provider, region)
		if bucket == "" {
			continue
		}
		for _, key := range keys {
			_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				return deleted, err
			}
			deleted++
		}
	}
	return deleted, nil
}

// reverseBundlePath attempts to derive the original bundle path from an S3 key
// by stripping the provider prefix and the file's own name/extension.
// For example: key="sound/bgm/bgm001/track.mp3" → "sound/bgm/bgm001"
func reverseBundlePath(provider config.StorageProviderConfig, region protocol.Region, key string) string {
	prefix := strings.ReplaceAll(provider.Prefix, "{region}", string(region))
	prefix = strings.ReplaceAll(prefix, "{server}", string(region))
	prefix = strings.Trim(prefix, "/")

	rel := key
	if prefix != "" {
		rel = strings.TrimPrefix(key, prefix+"/")
	}

	// The bundle path is the directory portion of the relative path.
	dir := filepath.ToSlash(filepath.Dir(rel))
	if dir == "." || dir == "" {
		return rel
	}
	return dir
}
