package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	ctx := context.Background()
	client, err := createS3Client(ctx)
	if err != nil {
		log.Fatal(err)
	}

	desktop := NewBucketDataNode(ctx, client, "desktop")
	main := NewBucketDataNode(ctx, client, "obsidian")

	err = SyncNode(main, desktop)
	if err != nil {
		fmt.Print(err)
	}
}

func createS3Client(ctx context.Context) (*s3.Client, error) {
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

// func gc() {}
