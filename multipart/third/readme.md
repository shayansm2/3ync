issues:
1. if two upload failes, one of the goroutine hangs
2. we do not cancel the other uploads

```go
func (u *multipartUploader) upload(parts <-chan part) (<-chan types.CompletedPart, <-chan struct{}) {
	out := make(chan types.CompletedPart)
	fail := make(chan struct{})

	numberOfWorkers := 2
	var wg sync.WaitGroup
	for i := range numberOfWorkers {
		wg.Go(func() {
			for part := range parts {
				// do upload
				if err != nil{ 
					fail <- struct{}{}
					return
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

	return out, fail
}
```

```go
func (u *multipartUploader) waitForCompletion(parts <-chan types.CompletedPart, fail <-chan struct{}) ([]types.CompletedPart, error) {
	completedParts := make([]types.CompletedPart, 0)
	for {
		select {
		case part, ok := <-parts:
			if !ok {
				return completedParts, nil
			}
			completedParts = append(completedParts, part)
		case <-fail:
			return completedParts, fmt.Errorf("upload failed")
		}
	}
}
```
