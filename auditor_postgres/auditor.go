/**
 * CREATE TABLE auditor_txs (
 *   id serial PRIMARY KEY,
 *   block_height integer NOT NULL,
 *   transaction_hash character varying(64) NOT NULL,
 *   identifier integer NOT NULL,
 *
 *   from_wallet character varying(35),
 *   to_wallet character varying(35),
 *   value numeric,
 *   purpose character varying(64)
 * );
 *
 * CREATE UNIQUE INDEX auditor_txs_key ON auditor_txs USING btree (block_height, transaction_hash, identifier);
 *
**/

package main

import (
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"database/sql"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

func fillBalCalcOptimized(txDb *sql.DB) {

	txn, err := txDb.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("auditor_txs", "block_height", "transaction_hash", "identifier", "from_wallet", "to_wallet", "value", "purpose"))
	if err != nil {
		log.Fatal(err)
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
				var fromWalletHash string
				var toWalletHash string
				var purposeLabel string
				switch purpose := rand.Intn(2); purpose {
				case 2:
					fromWalletHash = "empty"
					toWalletHash = walletNumber
					purposeLabel = "player-deposit"
				case 1:
					fromWalletHash = walletNumber
					toWalletHash = "empty"
					purposeLabel = "player-withdrawal"
				case 0:
					fromWalletHash = walletNumber
					toWalletHash = walletNumber
					purposeLabel = "change"
				}

				_, err = stmt.Exec(blockNumber, txHash, k, fromWalletHash, toWalletHash, rand.Intn(10), purposeLabel)
				if err != nil {
					log.Fatal(err)
				}

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

	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}

	log.Println(blockNumber, txNumber, total)
}

func balanceForOptimized(txDb *sql.DB) {

	start := time.Now()

	var txIndex int
	// wallets := make(map[string]int64)
	queryStmt, err := txDb.Prepare("SELECT block_height, transaction_hash, identifier, purpose, value FROM auditor_txs WHERE block_height >= $1 AND block_height <= $2;")
	if err != nil {
		log.Fatal(err)
	}
	rows, err := queryStmt.Query(391182, 446032)
	defer rows.Close()
	var total int

	for rows.Next() {
		var blockHeight int
		var txHash string
		var identifier int
		var purpose string
		var amount int

		if err := rows.Scan(&blockHeight, &txHash, &identifier, &purpose, &amount); err != nil {
			log.Fatal(err)
		}
		// log.Println(txHash)
		// amount, _ := strconv.ParseInt(row[4], 10, 64)
		switch purpose {
		case "player-withdrawal":
			total -= amount
		case "player-deposit":
			total += amount
		}
		txIndex++
	}

	elapsed := time.Since(start)
	log.Printf("Balance took %s", elapsed)

	// log.Println("Wallets count:", len(wallets))
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
	txDb, err := sql.Open("postgres", "postgres://postgres:postgres@127.0.0.1/postgres?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	// fillBalCalcOptimized(txDb)
	balanceForOptimized(txDb)
}
