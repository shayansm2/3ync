package main

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// last write wins
func replicate(source, replica DataNode) error {
	srcList, err := source.ListObjects()
	if err != nil {
		return err
	}
	srcObjects := make(map[string]types.Object)
	for _, obj := range srcList {
		srcObjects[*obj.Key] = obj
	}

	replList, err := replica.ListObjects()
	if err != nil {
		return err
	}
	// TODO: make it concurrent
	for _, replObj := range replList {
		// if object not exists in source but exists in replica
		if srcObj, found := srcObjects[*replObj.Key]; !found {
			// if creation time > last update -> create
			// else -> do nothing
			if source.GetLastUpdate().After(source.GetLastUpdate()) {
				fileContent, err := replica.Get(*replObj.Key)
				if err != nil {
					return fmt.Errorf("cannot sync %s: %e", *replObj.Key, err)
				}
				err = source.Create(*replObj.Key, fileContent)
				if err != nil {
					return fmt.Errorf("cannot sync %s: %e", *replObj.Key, err)
				}
				log.Printf("created %s", *replObj.Key)
			}
		} else { // both objects exists
			// if etags are equal -> do nothing
			// if source last modified is greater -> do nothing
			// if destination last modified is greater -> keep a backup + replace the file from destination
			if srcObj.ETag == replObj.ETag {
				continue
			} else if srcObj.LastModified.After(*replObj.LastModified) {
				continue
			} else {
				err := source.Backup(*srcObj.Key)
				if err != nil {
					return fmt.Errorf("cannot get backup %s: %e", *srcObj.Key, err)
				}
				fileContent, err := replica.Get(*replObj.Key)
				if err != nil {
					return fmt.Errorf("cannot sync %s: %e", *replObj.Key, err)
				}
				err = source.Create(*replObj.Key, fileContent)
				if err != nil {
					return fmt.Errorf("cannot sync %s: %e", *replObj.Key, err)
				}
				log.Printf("modified %s", *replObj.Key)
			}
			// TODO DO NOT DELETE
			delete(srcObjects, *replObj.Key)
		}
	}
	// if object exists in source but not in replica
	for _, srcObj := range srcObjects {
		// if creation time > last update -> do nothing
		if srcObj.LastModified.After(source.GetLastUpdate()) {
			continue
		}
		// else -> keep a backup + delte
		err := source.Backup(*srcObj.Key)
		if err != nil {
			return fmt.Errorf("cannot get backup %s: %e", *srcObj.Key, err)
		}
		err = source.Delete(*srcObj.Key)
		if err != nil {
			return fmt.Errorf("cannot sync %s: %e", *srcObj.Key, err)
		}
		log.Printf("deleted %s", *srcObj.Key)
	}
	return source.UpdateMetadata()
}

func syncObject(source, replica DataNode, srcObj, replObj *types.Object) error {
	// if object not exists in source but exists in replica
	if srcObj == nil {
		// if last modified < last update -> do nothing, will be deleted in next syncs
		if replObj.LastModified.Before(source.GetLastUpdate()) {
			return nil
		}
		// if last modified > last update -> create
		fileContent, err := replica.Get(*replObj.Key)
		if err != nil {
			return fmt.Errorf("cannot sync %s: %e", *replObj.Key, err)
		}
		err = source.Create(*replObj.Key, fileContent)
		if err != nil {
			return fmt.Errorf("cannot sync %s: %e", *replObj.Key, err)
		}
		log.Printf("created %s", *replObj.Key)
		return nil
	}
	// if object exists in source but not in replica
	if replObj == nil {
		// if last modified > last update -> keep, do nothing
		if srcObj.LastModified.After(source.GetLastUpdate()) {
			return nil
		}
		// else -> backup & delete
		err := source.Backup(*srcObj.Key)
		if err != nil {
			return fmt.Errorf("cannot get backup %s: %e", *srcObj.Key, err)
		}
		err = source.Delete(*srcObj.Key)
		if err != nil {
			return fmt.Errorf("cannot sync %s: %e", *srcObj.Key, err)
		}
		log.Printf("deleted %s", *srcObj.Key)
		return nil
	}
	// both objects exists
	// if etags are equal -> do nothing
	if srcObj.ETag == replObj.ETag {
		return nil
	}
	// if source last modified is greater -> do nothing
	if srcObj.LastModified.After(*replObj.LastModified) {
		return nil
	}
	// if destination last modified is greater -> backup + replace
	err := source.Backup(*srcObj.Key)
	if err != nil {
		return fmt.Errorf("cannot get backup %s: %e", *srcObj.Key, err)
	}
	fileContent, err := replica.Get(*replObj.Key)
	if err != nil {
		return fmt.Errorf("cannot sync %s: %e", *replObj.Key, err)
	}
	err = source.Create(*replObj.Key, fileContent)
	if err != nil {
		return fmt.Errorf("cannot sync %s: %e", *replObj.Key, err)
	}
	log.Printf("modified %s", *replObj.Key)
	return nil
}
