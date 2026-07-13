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

func TestReplication(t *testing.T) {
	ctx := context.Background()
	srcBucket := "source"
	replBucket := "replica"
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &srcBucket})
	require.NoError(t, err)
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &replBucket})
	require.NoError(t, err)
	srcNode := NewBucketDataNode(ctx, client, srcBucket)
	replNode := NewBucketDataNode(ctx, client, replBucket)

	t.Run("test create new file from replica", func(t *testing.T) {
		log.Println("TEST: create new file from replica")
		// pre conditions
		replBody := "created in replica, not exists in source"
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &replBucket,
			Key:    aws.String("replica-created"),
			Body:   strings.NewReader(replBody),
		})
		require.NoError(t, err)

		// test
		err = Replicate(srcNode, replNode)
		require.NoError(t, err)

		// assertions
		get, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: &srcBucket,
			Key:    aws.String("replica-created"),
		})
		assert.NoError(t, err)
		srcBody, err := io.ReadAll(get.Body)
		assert.NoError(t, err)
		assert.Equal(t, replBody, string(srcBody))
	})

	t.Run("test do not create deleted file from replica", func(t *testing.T) {
		log.Println("TEST: do not create deleted file from replica")
		// pre conditions
		client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: &srcBucket,
			Key:    aws.String("replica-created"),
		})

		// test
		err = Replicate(srcNode, replNode)
		require.NoError(t, err)

		// assertions
		_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: &srcBucket,
			Key:    aws.String("replica-created"),
		})
		assert.Error(t, err)
	})

	t.Run("test do not delete created file from source", func(t *testing.T) {
		log.Println("TEST: do not delete created file from source")
		// pre conditions
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &srcBucket,
			Key:    aws.String("source-created"),
			Body:   strings.NewReader("created in source, not exists in replica"),
		})
		require.NoError(t, err)

		// test
		err = Replicate(srcNode, replNode)
		require.NoError(t, err)

		// assertions
		_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: &srcBucket,
			Key:    aws.String("source-created"),
		})
		assert.NoError(t, err)
	})

	t.Run("test delete deleted file from replica", func(t *testing.T) {
		log.Println("TEST: delete deleted file from replica")
		// pre conditions
		err = Replicate(replNode, srcNode)
		require.NoError(t, err)

		_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: &replBucket,
			Key:    aws.String("source-created"),
		})
		require.NoError(t, err)

		// test
		err = Replicate(srcNode, replNode)
		require.NoError(t, err)

		// assertions
		_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: &srcBucket,
			Key:    aws.String("source-created"),
		})
		assert.Error(t, err)
	})

	t.Run("test do not sync identical objects", func(t *testing.T) {
		log.Println("TEST: do not sync identical objects")
		// pre conditions
		v1 := "v1"
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &srcBucket,
			Key:    aws.String("obj"),
			Body:   strings.NewReader(v1),
		})
		require.NoError(t, err)
		err = Replicate(replNode, srcNode)
		require.NoError(t, err)

		// test
		err = Replicate(srcNode, replNode)
		require.NoError(t, err)

		// assertion
		get, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: &srcBucket,
			Key:    aws.String("obj"),
		})
		assert.NoError(t, err)
		srcBody, err := io.ReadAll(get.Body)
		assert.NoError(t, err)
		assert.Equal(t, v1, string(srcBody))
	})

	t.Run("test do not replace newer object in source", func(t *testing.T) {
		log.Println("TEST: do not replace newer object in source")
		// pre conditions
		v2 := "v2"
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &srcBucket,
			Key:    aws.String("obj"),
			Body:   strings.NewReader(v2),
		})
		require.NoError(t, err)

		// test
		err = Replicate(srcNode, replNode)
		require.NoError(t, err)

		// assertion
		get, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: &srcBucket,
			Key:    aws.String("obj"),
		})
		assert.NoError(t, err)
		srcBody, err := io.ReadAll(get.Body)
		assert.NoError(t, err)
		assert.Equal(t, v2, string(srcBody))
	})

	t.Run("test replace newer object from replica", func(t *testing.T) {
		log.Println("TEST: replace newer object from replica")
		// pre conditions
		v3 := "v3"
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &replBucket,
			Key:    aws.String("obj"),
			Body:   strings.NewReader(v3),
		})
		require.NoError(t, err)

		// test
		err = Replicate(srcNode, replNode)
		require.NoError(t, err)

		// assertion
		get, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: &srcBucket,
			Key:    aws.String("obj"),
		})
		assert.NoError(t, err)
		srcBody, err := io.ReadAll(get.Body)
		assert.NoError(t, err)
		assert.Equal(t, v3, string(srcBody))
	})
}
