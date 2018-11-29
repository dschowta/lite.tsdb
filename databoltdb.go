package tsdb

import (
	"encoding/binary"
	"fmt"

	"github.com/boltdb/bolt"
)

type Boltdb struct {
	db *bolt.DB
}

func (bdb *Boltdb) Connect(path string) error {
	var err error
	//TODO: make the mode configurable
	bdb.db, err = bolt.Open(path, 0600, nil)
	return err
}

func (bdb Boltdb) Disconnect() error {
	return bdb.db.Close()
}

//This function converts a floating point number (which is supported by senml) to a bytearray
func timeToByteArr(timeVal int64) []byte {
	buff := make([]byte, 8)
	binary.BigEndian.PutUint64(buff, uint64(timeVal))

	return buff

}

//This function converts a bytearray floating point number (which is supported by senml)
func byteArrToTime(byteArr []byte) int64 {
	//This is set to bigendian so that the timestamp is sorted in binary format.
	timeVal := int64(binary.BigEndian.Uint64(byteArr))
	return timeVal
}
func (bdb Boltdb) Add(name string, timeseries TimeSeries) error {
	if name == "" {
		return fmt.Errorf("Time series record with Empty name")
	}
	if err := bdb.db.Batch(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return err
		}
		for _, entry := range timeseries {

			err = b.Put(timeToByteArr(entry.Time), entry.Value)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (bdb Boltdb) Get(series string) (TimeSeries, error) {
	timeSeries := make([]TimeEntry, 0, 100)
	err := bdb.db.View(func(tx *bolt.Tx) error {

		b := tx.Bucket([]byte(series))
		if b == nil {
			return fmt.Errorf("Bucket:%v does not exist", series)
		}

		err := b.ForEach(func(k, v []byte) error {

			record := TimeEntry{byteArrToTime(k), v}
			//TODO: 1. This is an inefficient way of keeping the slices. This has to be addressed during the pagination implementation
			timeSeries = append(timeSeries, record)
			return nil
		})
		if err != nil {
			return err
		}
		return err
	})

	if err != nil {
		return nil, err
	}

	return timeSeries, err
}

func (bdb Boltdb) GetOnChannel(series string) (<-chan TimeEntry, chan error) {

	resultCh := make(chan TimeEntry, 10)
	errorCh := make(chan error)
	go func() {
		//defer close(resultCh)
		defer close(errorCh)
		err := bdb.db.View(func(tx *bolt.Tx) error {

			b := tx.Bucket([]byte(series))
			if b == nil {
				return fmt.Errorf("Bucket:%v does not exist", series)
			}

			err := b.ForEach(func(k, v []byte) error {
				record := TimeEntry{byteArrToTime(k), v}
				resultCh <- record
				return nil
			})
			if err != nil {
				return err
			}
			return err
		})
		close(resultCh)
		if err != nil {
			errorCh <- err
			return
		}
	}()

	return resultCh, errorCh
}

func (bdb Boltdb) Delete(series string) error {
	return bdb.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(series))
	})
}
