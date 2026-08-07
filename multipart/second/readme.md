pipeline

```go
func (u *multipartUploader) upload(parts <-chan part) <-chan types.CompletedPart {
	out := make(chan types.CompletedPart)

	numberOfWorkers := 4
	var wg sync.WaitGroup
	for i := range numberOfWorkers {
		wg.Go(func() {...})
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
```

```
                         ┌────────────┐                                         
                   ┌────►│upload parts┼──┐                                      
                   │     └────────────┘  │                                      
                   │                     │                                      
┌──────────────┐   │     ┌────────────┐  │   ┌─────────────────────────────────┐
│generate parts┼───┼────►│upload parts├──┼──►│wait for part upload and complete│
└──────────────┘   │     └────────────┘  │   └─────────────────────────────────┘
                   │                     │                                      
                   │     ┌────────────┐  │                                      
                   └────►│upload parts┼──┘                                      
                         └────────────┘                                         
```

they took the same time for 1 worker and 4 worker because my upload link was bandwidth-limited
