package main

import (
	"context"
	"io"
	"log"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSyncEngine(t *testing.T) {
	ctx := context.Background()
	bucket1 := "bucket1"
	bucket2 := "bucket2"
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucket1})
	require.NoError(t, err)
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucket2})
	require.NoError(t, err)
	node1 := NewBucketDataNode(ctx, client, bucket1)
	node2 := NewBucketDataNode(ctx, client, bucket2)

	t.Run("test created file in one bucket will be created on the other", func(t *testing.T) {
		log.Println("TEST: created file in one bucket will be created on the other")
		// pre conditions
		replBody := "created in one bucket, not exists in the other"
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &bucket2,
			Key:    aws.String("obj1"),
			Body:   strings.NewReader(replBody),
		})
		require.NoError(t, err)

		// test
		err = Synchronize(node1, node2)
		require.NoError(t, err)

		// assertions
		get, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: &bucket1,
			Key:    aws.String("obj1"),
		})
		assert.NoError(t, err)
		srcBody, err := io.ReadAll(get.Body)
		assert.NoError(t, err)
		assert.Equal(t, replBody, string(srcBody))
	})

	t.Run("test deleted file not be recreated", func(t *testing.T) {
		log.Println("TEST: deleted file not be recreated")
		// pre conditions
		client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: &bucket1,
			Key:    aws.String("obj1"),
		})

		// test
		err = Synchronize(node1, node2)
		require.NoError(t, err)

		// assertions
		_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: &bucket1,
			Key:    aws.String("obj1"),
		})
		assert.Error(t, err)
	})

	t.Run("test created file will not delete", func(t *testing.T) {
		log.Println("TEST: created file will not delete")
		// pre conditions
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &bucket1,
			Key:    aws.String("obj2"),
			Body:   strings.NewReader("created in bucket 1, not exists in bucket 2"),
		})
		require.NoError(t, err)

		// test
		err = Synchronize(node1, node2)
		require.NoError(t, err)

		// assertions
		_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: &bucket1,
			Key:    aws.String("obj2"),
		})
		assert.NoError(t, err)
	})

	t.Run("test deleted file in one bucket will delete in other bucket", func(t *testing.T) {
		log.Println("TEST: deleted file in one bucket will delete in other bucket")
		// pre conditions
		_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: &bucket2,
			Key:    aws.String("obj2"),
		})
		require.NoError(t, err)

		// test
		err = Synchronize(node1, node2)
		require.NoError(t, err)

		// assertions
		_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: &bucket1,
			Key:    aws.String("obj2"),
		})
		assert.Error(t, err)
	})

	t.Run("test do not recreate deleted file", func(t *testing.T) {
		log.Println("TEST: do not recreate deleted file")
		// pre conditions
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &bucket2,
			Key:    aws.String("obj3"),
			Body:   strings.NewReader("will be deleted soon"),
		})
		require.NoError(t, err)
		err = Synchronize(node1, node2) //SYNC TIME: 2026-07-13 23:30:43.025844 +0330 +0330 m=+8.066448335
		require.NoError(t, err)
		_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: &bucket2,
			Key:    aws.String("obj3"),
		})
		require.NoError(t, err)

		// test
		err = Synchronize(node1, node2)
		require.NoError(t, err)

		// assertions
		_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: &bucket2,
			Key:    aws.String("obj3"),
		})
		assert.Error(t, err)
	})

	t.Run("test do not sync identical objects", func(t *testing.T) {
		log.Println("TEST: do not sync identical objects")
		// pre conditions
		v1 := "v1"
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &bucket1,
			Key:    aws.String("obj"),
			Body:   strings.NewReader(v1),
		})
		require.NoError(t, err)
		err = Synchronize(node2, node1)
		require.NoError(t, err)

		// test
		err = Synchronize(node1, node2)
		require.NoError(t, err)

		// assertion
		get, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: &bucket1,
			Key:    aws.String("obj"),
		})
		assert.NoError(t, err)
		srcBody, err := io.ReadAll(get.Body)
		assert.NoError(t, err)
		assert.Equal(t, v1, string(srcBody))
	})

	t.Run("test replace newer object", func(t *testing.T) {
		log.Println("TEST: replace newer object")
		// pre conditions
		v2 := "v2"
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &bucket1,
			Key:    aws.String("obj"),
			Body:   strings.NewReader(v2),
		})
		require.NoError(t, err)

		// test
		err = Synchronize(node1, node2)
		require.NoError(t, err)

		// assertion
		get, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: &bucket1,
			Key:    aws.String("obj"),
		})
		assert.NoError(t, err)
		srcBody, err := io.ReadAll(get.Body)
		assert.NoError(t, err)
		assert.Equal(t, v2, string(srcBody))

		get, err = client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: &bucket2,
			Key:    aws.String("obj"),
		})
		assert.NoError(t, err)
		srcBody, err = io.ReadAll(get.Body)
		assert.NoError(t, err)
		assert.Equal(t, v2, string(srcBody))
	})
}
