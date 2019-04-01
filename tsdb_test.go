package tsdb

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"testing"
	"time"
)

func setupDB(dbName string) (TSDB, string, error) {
	temp_file := fmt.Sprintf("senml_test_temp_%s", dbName)
	config := BoltDBConfig{Path: temp_file}

	db, err := Open(config)
	if err != nil {
		return nil, temp_file, err
	}
	return db, temp_file, nil
}

func clean(db TSDB, temp_filepath string) {
	db.Close()
	err := os.Remove(temp_filepath)
	if err != nil {
		fmt.Println(err.Error())
	}
}

func TestTsdb_Add(t *testing.T) {
	tname := "TestTsdb_Add"
	db, filePath, err := setupDB(tname)
	if err != nil {
		t.Fatal(err.Error())
	}

	defer clean(db, filePath)
	series := createDummyRecords(100, false)
	err = db.Add(tname, series)

	if err != nil {
		t.Fatal(err)
	}

	query := Query{Series: tname, MaxEntries: 100, Start: series[0].Time, End: series[len(series)-1].Time, Sort: ASC}
	resSeries, nextEntry, err := db.Query(query)
	if err != nil {
		t.Fatal(err)
	}

	if compareTimeSeries(resSeries, series) == false {
		t.Error("Inserted and Fetched did not match")
	}
	if nextEntry != nil {
		t.Error("nextEntry is null")
	}
}

func TestTsdb_CheckDescending(t *testing.T) {
	tname := "TestTsdb_CheckDescending"
	db, filePath, err := setupDB(tname)
	if err != nil {
		t.Fatal(err.Error())
	}

	defer clean(db, filePath)
	series := createDummyRecords(100, false)
	err = db.Add(tname, series)

	if err != nil {
		t.Fatal(err)
	}

	query := Query{Series: tname, MaxEntries: 100, Start: series[0].Time, End: series[len(series)-1].Time, Sort: DESC}
	resSeries, nextEntry, err := db.Query(query)
	if err != nil {
		t.Fatal(err)
	}

	//reverse the series
	for i := len(series)/2 - 1; i >= 0; i-- {
		opp := len(series) - 1 - i
		series[i], series[opp] = series[opp], series[i]
	}
	if compareTimeSeries(resSeries, series) == false {
		t.Error("Inserted and Fetched did not match")
	}
	if nextEntry != nil {
		t.Error("nextEntry is null")
	}
}

func TestTsdb_QueryPagination(t *testing.T) {
	tname := "TestTsdb_CheckDescending"
	db, filePath, err := setupDB(tname)
	if err != nil {
		t.Fatal(err.Error())
	}

	defer clean(db, filePath)
	series := createDummyRecords(100, false)
	err = db.Add(tname, series)

	query := Query{Series: tname, MaxEntries: 50, Start: series[0].Time, End: series[len(series)-1].Time, Sort: ASC}
	resSeries, nextEntry, err := db.Query(query)
	if err != nil {
		t.Fatal(err)
	}

	if compareTimeSeries(resSeries, series[0:50]) == false {
		t.Error("First page entries did not match")
	}

	if nextEntry == nil {
		t.Error("nextEntry is null")
	}
	query = Query{Series: tname, MaxEntries: 50, Start: *nextEntry, End: series[len(series)-1].Time, Sort: ASC}
	resSeries, nextEntry, err = db.Query(query)
	if err != nil {
		t.Fatal(err)
	}

	if compareTimeSeries(resSeries, series[50:100]) == false {
		t.Error("Second page entries did not match")
	}

	if nextEntry != nil {
		t.Error("nextEntry is null")
	}
}

func TestTsdb_QueryPages(t *testing.T) {
	tname := "TestTsdb_CheckDescending"
	limit := 25
	count := 100
	db, filePath, err := setupDB(tname)
	if err != nil {
		t.Fatal(err.Error())
	}

	defer clean(db, filePath)
	series := createDummyRecords(count, false)
	err = db.Add(tname, series)

	query := Query{Series: tname, MaxEntries: limit, Start: series[0].Time, End: series[len(series)-1].Time, Sort: ASC}
	pages, retCount, err := db.GetPages(query)
	if err != nil {
		t.Fatal(err)
	}

	if retCount != count {
		t.Error("Length of time series do not match")
	}

	if len(pages) != count/limit {
		t.Error("Number of pages is not as expected")
	}

	for i := 0; i < count/limit; i = i + 1 {
		if pages[i] != series[i*limit].Time {
			t.Errorf("Page indices are not matching %v != %v", pages[i], series[i*limit].Time)
		}
	}
}

func compareTimeSeries(s1, s2 TimeSeries) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i := 0; i < len(s1); i++ {
		e1 := s1[i]
		e2 := s2[i]
		if e1.Time != e2.Time || bytes.Compare(e1.Value, e2.Value) != 0 {
			return false
		}
	}

	return true
}

func createDummyRecords(count int, dec bool) TimeSeries {
	timeVal := time.Date(
		2009, 0, 0, 0, 0, 0, 0, time.UTC)

	timeseries := make(TimeSeries, 0, count)

	for i := 0; i < count; i++ {
		value, err := timeVal.MarshalBinary()
		if err != nil {
			log.Fatal(err)
			return nil
		}
		timeseries = append(timeseries, TimeEntry{timeVal.UnixNano(), value})
		if dec {
			timeVal = timeVal.Add(-time.Second)
		} else {
			timeVal = timeVal.Add(time.Second)
		}
	}
	return timeseries
}
