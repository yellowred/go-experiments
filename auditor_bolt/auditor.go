package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func fillBalCalcOptimized() {
	txDb, err := bolt.Open("my.db", 0600, nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer txDb.Close()

	txDb.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("MyBucket"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}

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

					err = b.Put([]byte(voutKey), []byte(voutData))
					total++
					if err != nil {
						log.Fatal("LevelDb error:", err)
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
		log.Println(blockNumber, txNumber, total)

		return nil
	})

}

func balanceForOptimized() {
	txDb, err := leveldb.OpenFile("tx.dat", nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer txDb.Close()

	wallets := make(map[string]int64)
	iter := txDb.NewIterator(&util.Range{Start: []byte("391182_"), Limit: []byte("446032_")}, nil)
	var txIndex int
	// iter := blocksDb.NewIterator(nil, nil)
	for iter.Next() {
		key := strings.Split(string(iter.Key()), "_")
		voutData := strings.Split(string(iter.Value()), "_")
		value, _ := strconv.ParseInt(voutData[2], 10, 64)
		// log.Println(vout)
		switch purpose := voutData[3]; purpose {
		case "player-withdrawal":
			wallets[key[1]] -= value
		case "player-deposit":
			wallets[key[1]] += value
		}
		txIndex++
	}

	iter.Release()
	err = iter.Error()
	if err != nil {
		log.Fatalln(err)
	}

	var total int64
	for _, wallet := range wallets {
		total += wallet
	}
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
	// blockWallets(100000)
	// fillBlocksAndValues()
	// fetchTheLevel()
	// balance()
	fillBalCalcOptimized()
	// balanceForOptimized()
}
