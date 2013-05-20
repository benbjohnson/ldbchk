package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/jmhodges/levigo"
	"runtime"
	"os"
)

//------------------------------------------------------------------------------
//
// Constants
//
//------------------------------------------------------------------------------

const (
	KeyCount = 40000000
	ValueLength = 1000
	ConcurrencyLevel = 10
)

//------------------------------------------------------------------------------
//
// Functions
//
//------------------------------------------------------------------------------

//--------------------------------------
// Main
//--------------------------------------

func main() {
	// Generate data if it doesn't exist.
	flag.Parse()
	if flag.NArg() == 0 {
		warn("usage: ldbchk PATH")
		os.Exit(1)
	}

	// Enable multicore.
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Generate database if missing.
	path := flag.Arg(0)
	_, err := os.Stat(path);
	exists := !os.IsNotExist(err)

	// Create database.
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open(path, opts)
	if err != nil {
		warn("ldbchk: Unable to open database: %v", err)
		os.Exit(1)
	}
	ro := levigo.NewReadOptions()
	defer ro.Close()
	wo := levigo.NewWriteOptions()
	defer wo.Close()

	// Generate data if necessary.
	if !exists {
		for i := 0; i<KeyCount; i++ {
			value := make([]byte, ValueLength)
			value[0] = byte(i % 255)
			if err = db.Put(wo, []byte(fmt.Sprintf("%020d", i)), value); err != nil {
				warn("ldbchk: Cannot insert value [%d]:\n%x", i, value)
				os.Exit(1)
			}
		}
	}
	
	// Test concurrent iteration.
	c := make(chan bool, ConcurrencyLevel)
	for i := 0; i < ConcurrencyLevel; i++ {
		go func() {
			iterator := db.NewIterator(ro)
			iterator.SeekToFirst()
			defer iterator.Close()

			index := 0
			for iterator = iterator; iterator.Valid(); iterator.Next() {
				//warn(fmt.Sprintf("%s", iterator.Key()))
				
				value := make([]byte, ValueLength)
				value[0] = byte(index % 255)
				if !bytes.Equal(iterator.Key(), []byte(fmt.Sprintf("%020d", index))) {
					warn("ldbchk: Invalid key: Expected %s, got %s", []byte(fmt.Sprintf("%020d", index)), iterator.Key())
				}
				if !bytes.Equal(iterator.Value(), value) {
					warn("ldbchk: Invalid Value: Expected %x, got %x", value[0:9], iterator.Value()[0:9])
				}
				index++
				
				if index % 100000 == 0 {
					fmt.Print(".")
				}
			}
			if err := iterator.GetError(); err != nil {
				warn("ldbchk: Iterator error: %v", err)
			}
			c <- true
		}()
	}
	
	// Wait for responses.
	for i := 0; i < ConcurrencyLevel; i++ {
		<-c
	}
	
	warn("DONE")
}


//--------------------------------------
// Utility
//--------------------------------------

// Writes to standard error.
func warn(msg string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", v...)
}
