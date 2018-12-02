package tsdb

import (
	"fmt"
	"os"
	"sync"
)

type BoltDBConfig struct {
	Path string
	Mode os.FileMode
}

type TimeEntry struct {
	Time  int64
	Value []byte
}

type TimeSeries []TimeEntry

type TSDB interface {

	//This function adds the senml records
	Add(name string, timeseries TimeSeries) error

	//Get the senml records
	Get(series string) (TimeSeries, error)

	//Returns two channels, one for Time entries and one for error.
	//This avoids the usage of an extra buffer by the database
	GetOnChannel(series string) (<-chan TimeEntry, chan error)

	//Delete a complete series
	Delete(series string) error

	//Close the database
	Close() error
}

var ds TSDB        //will be used as a singleton db object
var once sync.Once //make thread safe singleton

func Open(config interface{}) (TSDB, error) {
	switch config.(type) {
	case BoltDBConfig:
		retDB := new(Boltdb)
		err := retDB.open(config.(BoltDBConfig))
		return retDB, err
	default:
		return nil, fmt.Errorf("Unsupported storage Configuration")
	}
}
