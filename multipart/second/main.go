package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/joho/godotenv"
)

type multipartUploader struct {
	ctx      context.Context
	client   *s3.Client
	bucket   *string
	key      *string
	uploadId *string
}

func MultipartUpload(client *s3.Client, bucket string, key string, body io.Reader) error {
	uploader := multipartUploader{
		ctx:    context.Background(),
		bucket: aws.String(bucket),
		key:    aws.String(key),
		client: client,
	}
	err := uploader.init()
	if err != nil {
		return err
	}
	parts := uploader.generateParts(body)
	uploadedParts := uploader.upload(parts)
	err = uploader.complete(uploadedParts)

	return err
}

func (u *multipartUploader) init() error {
	resp, err := u.client.CreateMultipartUpload(u.ctx, &s3.CreateMultipartUploadInput{
		Bucket: u.bucket,
		Key:    u.key,
	})
	if err != nil {
		return err
	}
	log.Println("CreateMultipartUpload")
	u.uploadId = resp.UploadId
	return nil
}

const FiveMB = 5 * 1024 * 1024

type part struct {
	id   *int32
	body io.Reader
}

func (u *multipartUploader) generateParts(body io.Reader) <-chan part {
	out := make(chan part)
	go func() {
		defer close(out)
		for i := 1; ; i++ {
			buf := make([]byte, FiveMB)
			n, err := body.Read(buf)
			if n > 0 {
				out <- part{
					id:   aws.Int32(int32(i)),
					body: bytes.NewReader(buf[:n]),
				}
				log.Println("created part", i)
			}

			if err == io.EOF {
				return
			}
			if err != nil {
				// handle error
				return
			}
		}
	}()
	return out
}

func (u *multipartUploader) upload(parts <-chan part) <-chan types.CompletedPart {
	out := make(chan types.CompletedPart)

	numberOfWorkers := 4
	var wg sync.WaitGroup
	for i := range numberOfWorkers {
		wg.Go(func() {
			for part := range parts {
				resp, err := u.client.UploadPart(u.ctx, &s3.UploadPartInput{
					Bucket:     u.bucket,
					Key:        u.key,
					UploadId:   u.uploadId,
					Body:       part.body,
					PartNumber: part.id,
				})
				log.Println("[worker ", i, "]: UploadPart", *part.id)
				if err != nil {
					log.Println("ERROR", err)
				}
				out <- types.CompletedPart{
					ETag:       resp.ETag,
					PartNumber: part.id,
				}
			}
		})
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func (u *multipartUploader) complete(parts <-chan types.CompletedPart) error {
	completedParts := make([]types.CompletedPart, 0)
	for part := range parts {
		completedParts = append(completedParts, part)
	}
	_, err := u.client.CompleteMultipartUpload(u.ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          u.bucket,
		Key:             u.key,
		UploadId:        u.uploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completedParts},
	})
	log.Println("CompleteMultipartUpload")
	return err
}

func main() {
	godotenv.Load()
	ctx := context.Background()
	client, err := createS3Client(ctx)
	if err != nil {
		panic(err)
	}
	now := time.Now()
	// 4 parts
	err = MultipartUpload(client, "shayantest", "key2", strings.NewReader(strings.Repeat("-", 20*1024*1024)))
	if err != nil {
		panic(err)
	}
	fmt.Println("took", time.Since(now).Seconds(), "seconds")
}

func createS3Client(ctx context.Context) (*s3.Client, error) {
	if os.Getenv("ACCESS_KEY") == "" {
		return nil, fmt.Errorf("ACCESS_KEY not found in env variables")
	}
	if os.Getenv("SECRET_KEY") == "" {
		return nil, fmt.Errorf("SECRET_KEY not found in env variables")
	}
	if os.Getenv("BASE_END_POINT") == "" {
		return nil, fmt.Errorf("BASE_END_POINT not found in env variables")
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(os.Getenv("ACCESS_KEY"), os.Getenv("SECRET_KEY"), ""),
		),
		config.WithRegion("us-east-1"))
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(os.Getenv("BASE_END_POINT"))
		o.UsePathStyle = true
	})
	return client, nil
}
