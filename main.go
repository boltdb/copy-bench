package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/boltdb/bolt"
)

const batchSize = 10000
const itemCount = 4000000
const keySize = 8
const valueSize = 1024
const iteratePct = 0.2

func main() {
	log.SetFlags(0)
	flag.Parse()
	path := flag.Arg(0)
	if path == "" {
		log.Fatal("usage: copy-bench PATH")
	}

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	runtime.GOMAXPROCS(2)

	// Check if this is a new data file.
	_, err := os.Stat(path)
	isNew := os.IsNotExist(err)

	// Open database.
	db, err := bolt.Open(path, 0600)
	if err != nil {
		log.Fatal(err)
	}

	// Populate the initial database.
	if isNew {
		if err := seed(db); err != nil {
			log.Fatal(err)
		}
	}

	// Print stats of the db.
	if err := stat(db); err != nil {
		log.Fatal(err)
	}

	// Iterate once to push pages into memory.
	c := make(chan bool)
	fmt.Println("first run (ignore)")
	go func() { iterate(db, c) }()
	c <- true
	fmt.Println("")

	// Time iteration without copy.
	fmt.Println("iterate only")
	go iterate(db, c)
	time.Sleep(2 * time.Second)
	c <- true
	fmt.Println("")

	// Start iterator thread.
	fmt.Println("iterate during copy")
	go iterate(db, c)

	// Begin copy of the database.
	if err := dbcopy(db); err != nil {
		log.Fatal(err)
	}

	// Notify iterator of db copy completion.
	c <- true
	time.Sleep(100 * time.Millisecond)
}

// seed inserts an initial dataset into the database.
func seed(db *bolt.DB) error {
	log.Print("seeding")

	var count int
	var size int64
	for i := 0; i < itemCount; i += batchSize {
		err := db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("root"))
			if err != nil {
				return fmt.Errorf("create bucket: %s", err)
			}

			for j := 0; j < batchSize; j++ {
				k, v := make([]byte, keySize), make([]byte, valueSize)
				binary.BigEndian.PutUint64(k, uint64(count))
				if err := b.Put(k, v); err != nil {
					return fmt.Errorf("put: ", err)
				}
				count++
			}

			size = tx.Size()

			return nil
		})
		if err != nil {
			return err
		}
		log.Printf("  %d rows, %d bytes", count, size)
	}
	log.Print("(done)")
	fmt.Println("")

	if count != itemCount {
		return fmt.Errorf("invalid insert count: %d != %d", count, itemCount)
	}

	return nil
}

// stat prints out stats about the db.
func stat(db *bolt.DB) error {
	return db.View(func(tx *bolt.Tx) error {
		fmt.Printf("size: %d bytes\n", tx.Size())
		fmt.Println("")
		return nil
	})
}

// iterate continually loops over a subsection of the database and reads key/values.
func iterate(db *bolt.DB, c chan bool) {
	max := make([]byte, keySize)
	binary.BigEndian.PutUint64(max, uint64(itemCount*iteratePct))

	var d time.Duration
	var n int
loop:
	for {
		t := time.Now()

		// Loop over a subset of the data.
		var count int
		db.View(func(tx *bolt.Tx) error {
			c := tx.Bucket([]byte("root")).Cursor()
			for k, _ := c.First(); k != nil && bytes.Compare(k, max) == -1; k, _ = c.Next() {
				count++
			}
			return nil
		})
		log.Printf("  iterate: %v (n=%d)", time.Since(t), count)
		d += time.Since(t)
		n++

		// Check for completion.
		select {
		case <-c:
			break loop
		default:
		}
	}

	fmt.Printf("iterate: avg: %v (n=%d)\n", (d / time.Duration(n)), n)
}

// dbcopy performs a copy of the database file.
func dbcopy(db *bolt.DB) error {
	t := time.Now()
	err := db.View(func(tx *bolt.Tx) error {
		return tx.Copy(ioutil.Discard)
	})
	if err != nil {
		return err
	}
	fmt.Printf("copy: %v\n", time.Since(t))

	return nil
}
