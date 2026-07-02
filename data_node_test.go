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
	err = bdn.Create("test-obj", "Test Object")
	require.NoError(t, err)

	body, err := bdn.Get("test-obj")
	require.NoError(t, err)
	assert.Equal(t, "Test Object", body)

	err = bdn.Create("another-obj", "Another Object")
	require.NoError(t, err)

	objects, err := bdn.List()
	require.NoError(t, err)
	assert.Equal(t, 2, len(objects))

	assert.Equal(t, time.Time{}, bdn.GetLastUpdate())
	assert.Equal(t, time.Time{}, bdn.GetLastUpdate())
	err = bdn.UpdateMetadata()
	require.NoError(t, err)
	lastUpdate := bdn.GetLastUpdate()
	assert.NotEqual(t, time.Time{}, lastUpdate)

	bdn2 := NewBucketDataNode(ctx, client, bucketName)
	assert.Equal(t, lastUpdate, bdn2.GetLastUpdate())

	// Test List does not returns internal directories
	objects, err = bdn.List()
	require.NoError(t, err)
	assert.Equal(t, 2, len(objects))

	err = bdn.Backup("another-obj")
	require.NoError(t, err)
	backups, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: &bucketName,
		Prefix: aws.String(fmt.Sprintf("%s/backups", InternalDir)),
	})
	require.NoError(t, err)
	assert.Equal(t, 1, len(backups.Contents))
	assert.True(t, strings.HasSuffix(*backups.Contents[0].Key, "another-obj"))

	err = bdn.Delete("another-obj")
	require.NoError(t, err)
	objects, err = bdn.List()
	require.NoError(t, err)
	assert.Equal(t, 1, len(objects))
}
