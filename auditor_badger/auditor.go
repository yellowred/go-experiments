package main

import (
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger"
)

func fillBalCalcOptimized() {
	opts := badger.DefaultOptions
	opts.Dir = "./badger"
	opts.ValueDir = "./badger"
	txDb, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer txDb.Close()

	wb := txDb.NewWriteBatch()
	defer wb.Cancel()

	var blockNumber = 1
	var txNumber = 1
	var total = 0
	for blockNumber <= 1000000 {

		var txsInBlock = rand.Intn(40) // number of txs in a block
		var txs = []string{}
		var l = 1

		for l < txsInBlock {
			txHash := "txhash-" + strconv.Itoa(txNumber)
			txs = append(txs, txHash)

			var voutsInTx = 1 + rand.Intn(3)
			var k = 0
			for k < voutsInTx {
				var voutData = ""
				var walletNumber = strconv.Itoa(rand.Intn(1000000))
				var voutKey = cs(cs(strconv.Itoa(blockNumber), txHash), strconv.Itoa(k))
				switch purpose := rand.Intn(2); purpose {
				case 2:
					voutData = cs(cs(cs("empty", walletNumber), strconv.Itoa(rand.Intn(10))), "player-deposit")
				case 1:
					voutData = cs(cs(cs(walletNumber, "empty"), strconv.Itoa(rand.Intn(10))), "player-withdrawal")
				case 0:
					voutData = cs(cs(cs(walletNumber, walletNumber), strconv.Itoa(rand.Intn(10))), "change")
				}

				// err = txDb.Put([]byte(voutKey), []byte(voutData), nil)
				err := wb.Set([]byte(voutKey), []byte(voutData), 0)
				total++
				if err != nil {
					log.Fatal("Badger error:", err)
				} else {
					log.Println("Filled ", voutKey, voutData)
				}
				k++

			}
			l++
			txNumber++
		}
		blockNumber++
	}

	err = wb.Flush()
	if err != nil {
		log.Fatal("Badger error:", err)
	}

	log.Println(blockNumber, txNumber, total)
}

func balanceForOptimized(txDb *badger.DB) {

	start := time.Now()

	var txIndex int
	wallets := make(map[string]int64)
	txDb.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		for iter.Seek([]byte("391182_")); iter.Valid() && string(iter.Item().Key())[0:7] != "446032_"; iter.Next() {
			key := strings.Split(string(iter.Item().Key()), "_")
			var value []byte
			err := iter.Item().Value(func(v []byte) error {
				value = v
				return nil
			})
			if err != nil {
				log.Fatalln(err)
			}
			voutData := strings.Split(string(value), "_")
			amount, _ := strconv.ParseInt(voutData[2], 10, 64)
			// log.Println(vout)
			switch purpose := voutData[3]; purpose {
			case "player-withdrawal":
				wallets[key[1]] -= amount
			case "player-deposit":
				wallets[key[1]] += amount
			}
			txIndex++
		}
		return nil
	})

	var total int64
	for _, wallet := range wallets {
		total += wallet
	}

	elapsed := time.Since(start)
	log.Printf("Balance took %s", elapsed)

	log.Println("Wallets count:", len(wallets))
	log.Println("VOuts count:", txIndex)
	log.Println("Balance:", strconv.Itoa(int(total)))
}

func ats(arr []string) string {
	return strings.Join(arr, "_")
}

func cs(a string, b string) string {
	return strings.Join([]string{a, b}, "_")
}

func main() {
	opts := badger.DefaultOptions
	opts.Dir = "./badger"
	opts.ValueDir = "./badger"
	txDb, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer txDb.Close()

	// fillBalCalcOptimized()
	balanceForOptimized(txDb)
	balanceForOptimized(txDb)
	balanceForOptimized(txDb)
}
