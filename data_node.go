package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const MetadataFileName = ".metadata"
const DateFormat = "2006-01-02-15-04-05"

type Metadata struct {
	LastUpdate time.Time `json:"last_update"`
}

type DataNode interface {
	ListObjects() ([]types.Object, error)
	GetLastUpdate() time.Time
	Create(path, body string) error
	Get(path string) (string, error)
	Backup(path string) error
	Delete(path string) error
	UpdateMetadata() error
}

type BucketDataNode struct {
	name   string
	ctx    context.Context
	client *s3.Client
	now    time.Time
}

func NewBucketDataNode(ctx context.Context, client *s3.Client, name string) *BucketDataNode {
	return &BucketDataNode{
		name:   name,
		ctx:    ctx,
		client: &s3.Client{},
		now:    time.Now(),
	}
}

func (b *BucketDataNode) ListObjects() ([]types.Object, error) {
	list, err := b.client.ListObjectsV2(b.ctx, &s3.ListObjectsV2Input{Bucket: &b.name})
	if err != nil {
		return nil, fmt.Errorf("cannot list:%e", err)
	}
	return list.Contents, nil
}

// make it async once
func (b *BucketDataNode) GetLastUpdate() time.Time {
	obj, err := b.client.GetObject(b.ctx, &s3.GetObjectInput{
		Bucket: &b.name,
		Key:    aws.String(MetadataFileName),
	})
	if err != nil {
		return time.Time{}
	}

	var buf []byte
	n, err := obj.Body.Read(buf)
	if err != nil {
		return time.Time{}
	}

	var metadata Metadata
	err = json.Unmarshal(buf[:n], &metadata)
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
		return "", fmt.Errorf("cannot get: %e", err)
	}
	var buf []byte
	n, err := get.Body.Read(buf)
	if err != nil {
		return "", fmt.Errorf("cannot get: %e", err)
	}
	return string(buf[:n]), nil
}

func (b *BucketDataNode) Backup(path string) error {
	_, err := b.client.CopyObject(b.ctx, &s3.CopyObjectInput{
		Bucket:     &b.name,
		CopySource: &path,
		Key:        aws.String(fmt.Sprintf(".backup/%s/%s", b.now.UTC().Format(DateFormat), path)),
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

func (b *BucketDataNode) UpdateMetadata() error {
	metadata := Metadata{LastUpdate: b.now}
	content, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("cannot update metadata: %e", err)
	}
	_, err = b.client.PutObject(b.ctx, &s3.PutObjectInput{
		Bucket: &b.name,
		Key:    aws.String(MetadataFileName),
		Body:   bytes.NewReader(content),
	})
	return err
}
