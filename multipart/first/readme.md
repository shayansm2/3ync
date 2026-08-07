pipeline

```go
parts := uploader.generateParts(body)
uploadedParts := uploader.upload(parts)
uploader.complete(uploadedParts)
```


```
┌──────────────┐      ┌────────────┐      ┌─────────────────────────────────┐
│generate parts├─────►│upload parts├─────►│wait for part upload and complete│
└──────────────┘      └────────────┘      └─────────────────────────────────┘
```
