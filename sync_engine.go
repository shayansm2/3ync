package main

import (
	"log"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// last write wins
func syncNode(source, replica DataNode) error {
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
			wg.Go(func() { syncObject(source, replica, &srcObj, &replObj) })
			delete(srcObjects, key)
		} else {
			wg.Go(func() { syncObject(source, replica, nil, &replObj) })
		}
	}
	for _, srcObj := range srcObjects {
		wg.Go(func() { syncObject(source, replica, &srcObj, nil) })
	}
	wg.Wait()

	return source.UpdateMetadata()
}

func areIdentical(obj1, obj2 *types.Object) bool {
	// return *obj1.ETag == *obj2.ETag
	return *obj1.Key == *obj2.Key && *obj1.Size == *obj2.Size
}

func syncObject(source, replica DataNode, srcObj, replObj *types.Object) error {
	// if object not exists in source but exists in replica
	if srcObj == nil {
		// if last modified < last update -> do nothing, will be deleted in next syncs
		if replObj.LastModified.Before(source.GetLastUpdate()) {
			log.Printf("INFO: %s's LastModified in replica < lastUpdate: nothing to sync\n", *replObj.Key)
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
		if srcObj.LastModified.After(source.GetLastUpdate()) {
			log.Printf("INFO: %s's LastModified in source > lastUpdate: nothing to sync\n", *srcObj.Key)
			return nil
		}
		// else -> backup & delete
		err := source.Backup(*srcObj.Key)
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
	// TODO Fix checking identical objects
	if areIdentical(srcObj, replObj) {
		log.Printf("INFO: indentical objects %s: nothing to sync\n", *srcObj.Key)
		return nil
	}
	// if source last modified is greater -> do nothing
	if srcObj.LastModified.After(*replObj.LastModified) {
		log.Printf("INFO: %s's LastModified in source > lastUpdate :nothing to sync\n", *srcObj.Key)
		return nil
	}
	// if destination last modified is greater -> backup + replace
	err := source.Backup(*srcObj.Key)
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
