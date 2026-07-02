package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const InternalDir = ".3ync"
const DateFormat = "2006-01-02-15-04-05"

type Metadata struct {
	LastUpdate time.Time `json:"last_update"`
}

type DataNode interface {
	List() ([]types.Object, error)
	GetLastUpdate() time.Time
	Create(path, body string) error
	Get(path string) (string, error)
	Backup(path string, syncTime time.Time) error
	Delete(path string) error
	UpdateMetadata(syncTime time.Time) error
}

type BucketDataNode struct {
	name           string
	ctx            context.Context
	client         *s3.Client
	lastUpdate     time.Time
	lastUpdateOnce sync.Once
}

func NewBucketDataNode(ctx context.Context, client *s3.Client, name string) *BucketDataNode {
	return &BucketDataNode{
		name:   name,
		ctx:    ctx,
		client: client,
	}
}

func (b *BucketDataNode) List() ([]types.Object, error) {
	list, err := b.client.ListObjectsV2(b.ctx, &s3.ListObjectsV2Input{
		Bucket: &b.name,
	})
	if err != nil {
		return nil, fmt.Errorf("cannot list:%s \n", err)
	}
	result := make([]types.Object, 0)
	for _, obj := range list.Contents {
		if strings.HasPrefix(*obj.Key, InternalDir) {
			continue
		}
		result = append(result, obj)
	}
	return result, nil
}

func (b *BucketDataNode) GetLastUpdate() time.Time {
	b.lastUpdateOnce.Do(func() {
		b.lastUpdate = b.getLastUpdate()
	})
	return b.lastUpdate
}

func (b *BucketDataNode) getLastUpdate() time.Time {
	obj, err := b.client.GetObject(b.ctx, &s3.GetObjectInput{
		Bucket: &b.name,
		Key:    aws.String(fmt.Sprintf("%s/metadata.json", InternalDir)),
	})
	if err != nil {
		return time.Time{}
	}

	body, err := io.ReadAll(obj.Body)
	if err != nil {
		return time.Time{}
	}

	var metadata Metadata
	err = json.Unmarshal(body, &metadata)
	if err != nil {
		return time.Time{}
	}
	return metadata.LastUpdate
}

func (b *BucketDataNode) Create(path, body string) error {
	_, err := b.client.PutObject(b.ctx, &s3.PutObjectInput{
		Bucket: &b.name,
		Key:    &path,
		Body:   strings.NewReader(body),
	})
	return err
}

func (b *BucketDataNode) Get(path string) (string, error) {
	get, err := b.client.GetObject(b.ctx, &s3.GetObjectInput{
		Bucket: &b.name,
		Key:    &path,
	})
	if err != nil {
		return "", fmt.Errorf("cannot get object: %s", err)
	}
	body, err := io.ReadAll(get.Body)
	if err != nil {
		return "", fmt.Errorf("cannot read object: %s", err)
	}
	return string(body), nil
}

func (b *BucketDataNode) Backup(path string, syncTime time.Time) error {
	_, err := b.client.CopyObject(b.ctx, &s3.CopyObjectInput{
		// source
		CopySource: aws.String(fmt.Sprintf("%s/%s", b.name, path)),
		// destination
		Bucket: &b.name,
		Key:    aws.String(fmt.Sprintf("%s/backups/%s/%s", InternalDir, syncTime.Format(DateFormat), path)),
	})
	return err
}

func (b *BucketDataNode) Delete(path string) error {
	_, err := b.client.DeleteObject(b.ctx, &s3.DeleteObjectInput{
		Bucket: &b.name,
		Key:    aws.String(path),
	})
	return err
}

func (b *BucketDataNode) UpdateMetadata(syncTime time.Time) error {
	metadata := Metadata{LastUpdate: syncTime.UTC()}
	content, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("cannot update metadata: %s", err)
	}
	_, err = b.client.PutObject(b.ctx, &s3.PutObjectInput{
		Bucket: &b.name,
		Key:    aws.String(fmt.Sprintf("%s/metadata.json", InternalDir)),
		Body:   bytes.NewReader(content),
	})
	if err != nil {
		return err
	}
	b.lastUpdate = syncTime.UTC()
	return nil
}
