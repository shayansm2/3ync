package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBucketDataNode(t *testing.T) {
	ctx := context.Background()
	bucketName := "test"
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucketName})
	require.NoError(t, err)
	bdn := NewBucketDataNode(ctx, client, bucketName)

	t.Run("test create object", func(t *testing.T) {
		err = bdn.Create("test-obj", "Test Object")
		require.NoError(t, err)
	})

	t.Run("test get object", func(t *testing.T) {
		body, err := bdn.Get("test-obj")
		require.NoError(t, err)
		assert.Equal(t, "Test Object", body)
	})

	t.Run("test list objects", func(t *testing.T) {
		err = bdn.Create("another-obj", "Another Object")
		require.NoError(t, err)

		objects, err := bdn.List()
		require.NoError(t, err)
		assert.Equal(t, 2, len(objects))
	})

	t.Run("test last_update metadata", func(t *testing.T) {
		assert.Equal(t, time.Time{}, bdn.GetLastUpdate("fake"))
		assert.Equal(t, time.Time{}, bdn.GetLastUpdate("fake"))
		err = bdn.UpdateMetadata(time.Now(), "fake")
		require.NoError(t, err)
		lastUpdate := bdn.GetLastUpdate("fake")
		assert.NotEqual(t, time.Time{}, lastUpdate)

		bdn2 := NewBucketDataNode(ctx, client, bucketName)
		assert.Equal(t, lastUpdate, bdn2.GetLastUpdate("fake"))
	})

	t.Run("test list does not returns internal directories", func(t *testing.T) {
		objects, err := bdn.List()
		require.NoError(t, err)
		assert.Equal(t, 2, len(objects))
	})

	t.Run("test backup object", func(t *testing.T) {
		err = bdn.Backup("another-obj", time.Now())
		require.NoError(t, err)
		backups, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: &bucketName,
			Prefix: aws.String(fmt.Sprintf("%s/backups", InternalDir)),
		})
		require.NoError(t, err)
		assert.Equal(t, 1, len(backups.Contents))
		assert.True(t, strings.HasSuffix(*backups.Contents[0].Key, "another-obj"))
	})

	t.Run("test delete object", func(t *testing.T) {
		err = bdn.Delete("another-obj")
		require.NoError(t, err)
		objects, err := bdn.List()
		require.NoError(t, err)
		assert.Equal(t, 1, len(objects))
	})
}
