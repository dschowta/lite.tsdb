package tsdb

import (
	"fmt"
	"sync"
)

const (
	BOLTDB = iota
)

type TimeEntry struct {
	Time  int64
	Value []byte
}

type TimeSeries []TimeEntry

type TSDB interface {
	//before any operation, database need to be connected
	Connect(path string) error
	//This function adds the senml records
	Add(name string, timeseries TimeSeries) error

	//Get the senml records
	Get(series string) (TimeSeries, error)

	//Returns two channels, one for Time entries and one for error.
	//This avoids the usage of an extra buffer by the database
	GetOnChannel(series string) (<- chan TimeEntry, chan error)

	//Delete a complete series
	Delete(series string) error

	//Disconnect the database
	Disconnect() error
}

var ds TSDB        //will be used as a singleton db object
var once sync.Once //make thread safe singleton

func NewTSDB(storage int) (TSDB, error) {
	var err error
	once.Do(func() {
		ds, err = databaseFactory(storage)
	})
	return ds, err
}

func databaseFactory(storage int) (TSDB, error) {
	switch storage {
	case BOLTDB:
		return new(Boltdb), nil
	default:
		return nil, fmt.Errorf("Unsupported storage :%v", storage)
	}
}
