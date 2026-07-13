package main

import (
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// last write wins
func Replicate(source, replica DataNode) error {
	backupTs := time.Now()
	srcList, err := source.List()
	if err != nil {
		return err
	}
	srcObjects := make(map[string]types.Object)
	for _, obj := range srcList {
		srcObjects[*obj.Key] = obj
	}

	replList, err := replica.List()
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	for _, replObj := range replList {
		key := *replObj.Key
		if srcObj, found := srcObjects[key]; found {
			wg.Go(func() { replicateObject(source, replica, &srcObj, &replObj, backupTs) })
			delete(srcObjects, key)
		} else {
			wg.Go(func() { replicateObject(source, replica, nil, &replObj, backupTs) })
		}
	}
	for _, srcObj := range srcObjects {
		wg.Go(func() { replicateObject(source, replica, &srcObj, nil, backupTs) })
	}
	wg.Wait()

	return source.UpdateMetadata(time.Now(), replica.Name())
}

func replicateObject(source, replica DataNode, srcObj, replObj *types.Object, backupTs time.Time) error {
	// if object not exists in source but exists in replica
	if srcObj == nil {
		// if last modified < last update -> do nothing, will be deleted in next syncs
		if replObj.LastModified.Before(source.GetLastUpdate(replica.Name())) {
			log.Printf("INFO: %s's replica.LastModified < source.LastUpdate: nothing to sync\n", *replObj.Key)
			return nil
		}
		// if last modified > last update -> create
		fileContent, err := replica.Get(*replObj.Key)
		if err != nil {
			log.Printf("ERROR: cannot sync %s: %s\n", *replObj.Key, err)
			return err
		}
		err = source.Create(*replObj.Key, fileContent)
		if err != nil {
			log.Printf("ERROR: cannot sync %s: %s\n", *replObj.Key, err)
			return err
		}
		log.Printf("INFO: created %s\n", *replObj.Key)
		return nil
	}
	// if object exists in source but not in replica
	if replObj == nil {
		// if last modified > last update -> keep, do nothing
		if srcObj.LastModified.After(replica.GetLastUpdate(source.Name())) {
			log.Printf("INFO: %s's source.LastModified > replica.lastUpdate: nothing to sync\n", *srcObj.Key)
			return nil
		}
		// else -> backup & delete
		err := source.Backup(*srcObj.Key, backupTs)
		if err != nil {
			log.Printf("ERROR: cannot get backup %s: %s\n", *srcObj.Key, err)
			return err
		}
		err = source.Delete(*srcObj.Key)
		if err != nil {
			log.Printf("ERROR: cannot sync %s: %s\n", *srcObj.Key, err)
			return err
		}
		log.Printf("INOF: deleted %s\n", *srcObj.Key)
		return nil
	}
	// both objects exists
	// if etags are equal -> do nothing
	if *srcObj.ETag == *replObj.ETag {
		log.Printf("INFO: indentical objects %s: nothing to sync\n", *srcObj.Key)
		return nil
	}
	// if source last modified is greater -> do nothing
	if srcObj.LastModified.After(*replObj.LastModified) {
		log.Printf("INFO: %s's source.LastModified > replica.LastModified :nothing to sync\n", *srcObj.Key)
		return nil
	}
	// if destination last modified is greater -> backup + replace
	err := source.Backup(*srcObj.Key, backupTs)
	if err != nil {
		log.Printf("ERROR: cannot get backup %s: %s\n", *srcObj.Key, err)
		return err
	}
	fileContent, err := replica.Get(*replObj.Key)
	if err != nil {
		log.Printf("ERROR: cannot sync %s: %s\n", *replObj.Key, err)
		return err
	}
	err = source.Create(*replObj.Key, fileContent)
	if err != nil {
		log.Printf("ERROR: cannot sync %s: %s\n", *replObj.Key, err)
		return err
	}
	log.Printf("modified %s\n", *replObj.Key)
	return nil
}
