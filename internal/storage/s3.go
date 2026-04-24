package storage

import (
	"context"
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"moe-asset-server/internal/config"
	"moe-asset-server/internal/protocol"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Uploader struct {
	cfg *config.Config
}

func NewUploader(cfg *config.Config) *Uploader {
	return &Uploader{cfg: cfg}
}

func (u *Uploader) UploadManifestFiles(ctx context.Context, baseDir string, region protocol.Region, files []protocol.ResultFile) error {
	if len(files) == 0 {
		return nil
	}
	if len(u.cfg.Storage.Providers) == 0 {
		return fmt.Errorf("no storage providers configured")
	}
	for _, provider := range u.cfg.Storage.Providers {
		if provider.Kind != "" && !strings.EqualFold(provider.Kind, "s3") {
			continue
		}
		if err := u.uploadToProvider(ctx, provider, baseDir, region, files); err != nil {
			return err
		}
	}
	return nil
}

func (u *Uploader) uploadToProvider(ctx context.Context, provider config.StorageProviderConfig, baseDir string, region protocol.Region, files []protocol.ResultFile) error {
	client := s3Client(provider)
	bucket := strings.ReplaceAll(provider.Bucket, "{region}", string(region))
	bucket = strings.ReplaceAll(bucket, "{server}", string(region))
	if bucket == "" {
		return fmt.Errorf("s3 bucket is empty for provider %s", provider.Endpoint)
	}

	concurrency := u.cfg.Storage.UploadConcurrency
	if concurrency <= 0 {
		concurrency = 16
	}
	sem := make(chan struct{}, concurrency)
	errCh := make(chan error, len(files))
	var wg sync.WaitGroup
	for _, f := range files {
		fileInfo := f
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := uploadOne(ctx, client, provider, bucket, baseDir, region, fileInfo); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		return err
	}
	return nil
}

func s3Client(provider config.StorageProviderConfig) *s3.Client {
	region := provider.Region
	if region == "" {
		region = "us-east-1"
	}
	cfg := aws.Config{
		BaseEndpoint: aws.String(endpointURL(provider.Endpoint, provider.TLS)),
		Region:       region,
		Credentials:  credentials.NewStaticCredentialsProvider(provider.AccessKey, provider.SecretKey, ""),
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = provider.PathStyle
	})
}

func uploadOne(ctx context.Context, client *s3.Client, provider config.StorageProviderConfig, bucket string, baseDir string, region protocol.Region, resultFile protocol.ResultFile) error {
	localPath := filepath.Join(baseDir, filepath.FromSlash(resultFile.Path))
	if !isSafeRelativePath(resultFile.Path) {
		return fmt.Errorf("unsafe result path %q", resultFile.Path)
	}
	remotePath := remoteKey(provider, region, resultFile.Path)

	if provider.Dedupe.Enabled && provider.Dedupe.VerifyRemote {
		head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(remotePath),
		})
		if err == nil && head.ContentLength != nil && *head.ContentLength == resultFile.Size {
			return nil
		}
	}

	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", localPath, err)
	}
	defer func() { _ = file.Close() }()

	contentType := mime.TypeByExtension(filepath.Ext(localPath))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	input := &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(remotePath),
		Body:          file,
		ContentType:   aws.String(contentType),
		ContentLength: aws.Int64(resultFile.Size),
	}
	if provider.PublicRead {
		input.ACL = types.ObjectCannedACLPublicRead
	}
	if _, err := client.PutObject(ctx, input); err != nil {
		return fmt.Errorf("upload %s to s3://%s/%s: %w", localPath, bucket, remotePath, err)
	}
	return nil
}

func endpointURL(endpoint string, tls bool) string {
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		return endpoint
	}
	if tls {
		return "https://" + endpoint
	}
	return "http://" + endpoint
}

func remoteKey(provider config.StorageProviderConfig, region protocol.Region, relPath string) string {
	prefix := strings.ReplaceAll(provider.Prefix, "{region}", string(region))
	prefix = strings.ReplaceAll(prefix, "{server}", string(region))
	prefix = strings.Trim(prefix, "/")
	relPath = strings.TrimPrefix(filepath.ToSlash(relPath), "/")
	if prefix == "" {
		return relPath
	}
	return prefix + "/" + relPath
}

func isSafeRelativePath(path string) bool {
	if path == "" || strings.Contains(path, "\\") || strings.Contains(path, ":") {
		return false
	}
	cleaned := filepath.ToSlash(filepath.Clean(filepath.FromSlash(path)))
	return cleaned == path && !strings.HasPrefix(cleaned, "../") && cleaned != ".." && !strings.HasPrefix(cleaned, "/")
}
