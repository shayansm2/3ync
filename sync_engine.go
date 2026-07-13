package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// last write wins
func Synchronize(first, second DataNode) error {
	backupTime := time.Now()
	List1, err := first.List()
	if err != nil {
		return err
	}
	Objects1 := make(map[string]types.Object)
	for _, obj := range List1 {
		Objects1[*obj.Key] = obj
	}

	List2, err := second.List()
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	for _, obj2 := range List2 {
		key := *obj2.Key
		if obj1, found := Objects1[key]; found {
			wg.Go(func() { syncObject(first, second, &obj1, &obj2, backupTime) })
			delete(Objects1, key)
		} else {
			wg.Go(func() { syncObject(first, second, nil, &obj2, backupTime) })
		}
	}
	for _, obj := range Objects1 {
		wg.Go(func() { syncObject(first, second, &obj, nil, backupTime) })
	}
	wg.Wait()

	syncTime := time.Now()
	err = first.UpdateMetadata(syncTime, second.Name())
	if err != nil {
		return err
	}
	// todo: rollback left
	return second.UpdateMetadata(syncTime, first.Name())
}

func syncObject(first, second DataNode, obj1, obj2 *types.Object, backupTime time.Time) error {
	if obj1 != nil && obj2 != nil {
		if *obj1.ETag == *obj2.ETag {
			// log.Printf("INFO: indentical objects %s: nothing to sync\n", *leftObj.Key)
			return nil
		}
		var old, new DataNode
		var oldObj, newObj types.Object
		if obj1.LastModified.After(*obj2.LastModified) {
			log.Printf(
				"INFO: %s's %s.LastModified > %s.LastModified :update %s\n",
				*obj1.Key, first.Name(), second.Name(), second.Name(),
			)
			new, old = first, second
			newObj, oldObj = *obj1, *obj2
		} else {
			log.Printf(
				"INFO: %s's %s.LastModified < %s.LastModified :update %s\n",
				*obj1.Key, first.Name(), second.Name(), first.Name(),
			)
			new, old = second, first
			newObj, oldObj = *obj2, *obj1
		}
		err := backupAndReplace(new, old, &newObj, &oldObj, backupTime)
		if err != nil {
			log.Printf("ERROR: cannot sync %s in %s: %s\n", *newObj.Key, old.Name(), err)
			return err
		}
		log.Printf("INFO: modified %s in %s\n", *newObj.Key, old.Name())
		return nil
	}
	var miss, exist DataNode
	var obj *types.Object
	if obj1 == nil {
		miss, exist = first, second
		obj = obj2
	} else {
		miss, exist = second, first
		obj = obj1
	}
	if obj.LastModified.Before(miss.GetLastUpdate(exist.Name())) {
		log.Printf(
			"INFO: %s.%s.LastModified < %s.LastUpdate :delete %s from %s\n",
			exist.Name(), *obj.Key, miss.Name(), *obj.Key, exist.Name(),
		)
		err := backupAndDelete(exist, obj, backupTime)
		if err != nil {
			log.Printf("ERROR: cannot delete %s in %s: %s\n", *obj.Key, exist.Name(), err)
			return err
		}
		log.Printf("INFO: deleted %s in %s\n", *obj.Key, exist.Name())
		return nil
	}

	log.Printf(
		"INFO: %s.%s.LastModified < %s.LastUpdate :create %s in %s\n",
		exist.Name(), *obj.Key, miss.Name(), *obj.Key, miss.Name(),
	)
	err := create(exist, miss, obj)
	if err != nil {
		log.Printf("ERROR: cannot create %s in %s: %s\n", *obj.Key, miss.Name(), err)
		return err
	}
	log.Printf("INFO: created %s in %s\n", *obj.Key, miss.Name())
	return nil
}

func backupAndReplace(new, old DataNode, newObj, oldObj *types.Object, backupTime time.Time) error {
	err := old.Backup(*oldObj.Key, backupTime)
	if err != nil {
		return fmt.Errorf("ERROR: cannot get backup from %s in %s: %s\n", *oldObj.Key, old.Name(), err)
	}
	fileContent, err := new.Get(*newObj.Key)
	if err != nil {
		return fmt.Errorf("ERROR: cannot get %s from %s: %s\n", *newObj.Key, new.Name(), err)
	}
	err = old.Create(*newObj.Key, fileContent)
	if err != nil {
		log.Printf("ERROR: cannot create %s in %s: %s\n", *newObj.Key, old.Name(), err)
		return err
	}
	return nil
}

func backupAndDelete(node DataNode, obj *types.Object, backupTime time.Time) error {
	err := node.Backup(*obj.Key, backupTime)
	if err != nil {
		return fmt.Errorf("ERROR: cannot get backup %s in %s: %s\n", *obj.Key, node.Name(), err)
	}
	err = node.Delete(*obj.Key)
	if err != nil {
		return fmt.Errorf("ERROR: cannot delete %s in %s: %s\n", *obj.Key, node.Name(), err)
	}
	return nil
}

func create(src, dst DataNode, obj *types.Object) error {
	fileContent, err := src.Get(*obj.Key)
	if err != nil {
		return fmt.Errorf("ERROR: cannot get %s from %s: %s\n", *obj.Key, src.Name(), err)
	}
	err = dst.Create(*obj.Key, fileContent)
	if err != nil {
		return fmt.Errorf("ERROR: cannot create %s in %s: %s\n", *obj.Key, dst.Name(), err)
	}
	return nil
}
