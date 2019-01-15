package tsdb

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/boltdb/bolt"
)

type Boltdb struct {
	db *bolt.DB
}

func (bdb *Boltdb) open(config BoltDBConfig) error {
	var err error
	mode := config.Mode
	if mode == 0 {
		mode = 0600
	}
	bdb.db, err = bolt.Open(config.Path, mode, nil)
	return err
}

func (bdb Boltdb) Close() error {
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
		return fmt.Errorf("Time Series record with Empty name")
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

func (bdb Boltdb) Query(q Query) (QueryResult, error) {
	timeSeries := make([]TimeEntry, 0, q.Limit)
	var nextEntry *int64
	nextEntry = nil
	err := bdb.db.View(func(tx *bolt.Tx) error {

		b := tx.Bucket([]byte(q.Series))
		if b == nil {
			return fmt.Errorf("Bucket:%v does not exist", q.Series)
		}

		c := b.Cursor()

		//Default case : If the sorting is descending
		first := q.End
		last := q.Start
		next := c.Prev
		loopCondition := func(val int64, last int64) bool {
			return val >= last
		}
		//else
		if strings.Compare(q.Sort, ASC) == 0 {
			first = q.Start
			last = q.End
			next = c.Next
			loopCondition = func(val int64, last int64) bool {
				return val <= last
			}

		}

		count := 0
		// Iterate over the time values
		var k, v []byte
		for k, v = c.Seek(timeToByteArr(first)); k != nil && loopCondition(byteArrToTime(k), last) && count < q.Limit; k, v = next() {
			record := TimeEntry{byteArrToTime(k), v}
			timeSeries = append(timeSeries, record)
			count = count + 1
		}
		if count == q.Limit && k != nil && loopCondition(byteArrToTime(k), last) {
			ne := byteArrToTime(k)
			nextEntry = &ne
		}
		return nil
	})

	if err != nil {
		return QueryResult{}, err
	}

	return QueryResult{Series: timeSeries, NextEntry: nextEntry}, nil
}

func (bdb Boltdb) GetPages(q Query) ([]int64, error) {
	keyList := make([]int64, 0, 100)

	err := bdb.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(q.Series))
		if b == nil {
			return fmt.Errorf("Bucket:%v does not exist", q.Series)
		}

		c := b.Cursor()

		first := q.End
		last := q.Start
		next := c.Prev
		loopCondition := func(val int64, last int64) bool {
			return val >= last
		}
		if strings.Compare(q.Sort, ASC) == 0 {
			first = q.Start
			last = q.End
			next = c.Next
			loopCondition = func(val int64, last int64) bool {
				return val <= last
			}

		}

		// Iterate over the time values
		var k []byte
		count := 0

		for k, _ = c.Seek(timeToByteArr(first)); k != nil && loopCondition(byteArrToTime(k), last); k, _ = next() {
			if count == 0 {
				count = 0
				keyList = append(keyList, byteArrToTime(k))
			}
			count = (count + 1) % q.Limit
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return keyList, nil
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
		//make sure you close the resultchannel before error channel
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