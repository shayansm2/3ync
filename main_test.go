package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var client *s3.Client

func TestMain(m *testing.M) {
	ctx := context.Background()
	minio, err := testcontainers.Run(
		ctx, "minio/minio:RELEASE.2025-09-07T16-13-09Z-cpuv1",
		testcontainers.WithExposedPorts("9000/tcp", "9001/tcp"),
		testcontainers.WithCmd(
			"server",
			"/data",
			"--address",
			":9000",
			"--console-address",
			":9001",
		),
		testcontainers.WithName("test-minio"),
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/minio/health/ready").WithPort("9000/tcp"),
		),
	)
	if err != nil {
		panic(err)
	}

	port, err := minio.MappedPort(ctx, "9000")
	if err != nil {
		panic(err)
	}

	os.Setenv("ACCESS_KEY", "minioadmin")
	os.Setenv("SECRET_KEY", "minioadmin")
	os.Setenv("BASE_END_POINT", fmt.Sprintf("http://localhost:%d", port.Num()))

	client, err = createS3Client(ctx)
	if err != nil {
		panic(err)
	}

	code := m.Run()
	if code != 0 {
		fmt.Println()
		port, err = minio.MappedPort(ctx, "9001")
		if err != nil {
			return
		}
		fmt.Printf("you can check out http://localhost:%d for debugging", port.Num())
		time.Sleep(time.Minute)
	}
}
