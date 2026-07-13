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
		log.Println("WARN: no .env file found, relying on env variables")
	}

	if len(os.Args) != 3 {
		log.Fatal("usage: 3ync <source_bucket> <replica_bucket>")
	}

	ctx := context.Background()
	client, err := createS3Client(ctx)
	if err != nil {
		log.Fatal(err)
	}

	srcBucket := os.Args[1]
	replicaBucket := os.Args[2]

	err = Synchronize(
		NewBucketDataNode(ctx, client, srcBucket),
		NewBucketDataNode(ctx, client, replicaBucket),
	)
	if err != nil {
		log.Fatal(err)
	}
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

// func gc() {}
