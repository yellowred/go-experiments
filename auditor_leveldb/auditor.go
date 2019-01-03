package main

import (
	"encoding/base64"
	"fmt"
	"log"
	"math/rand"
	"os/exec"
	"strconv"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/ybbus/jsonrpc"
)

type BitcoindResponse struct {
	Result string `json:"result"`
	Error  int    `json:"error"`
	ReqId  string `json:"id"`
}

type BlockInfoResponse struct {
	Txs       []string `json:"tx"`
	TxsNumber int      `json:"nTx"`
}

func blockHash(height int) {
	cmd := exec.Command("docker", "exec", "bitcoind-node", "bitcoin-cli", "getblockhash", "10000")
	stdout, err := cmd.CombinedOutput()

	if err != nil {
		fmt.Println(fmt.Sprint(err) + ": " + string(stdout))
		return
	}
	println(string(stdout))
}

func blockWallets(height int) {
	rpcClient := jsonrpc.NewClientWithOpts("http://127.0.0.1:8332", &jsonrpc.RPCClientOpts{
		CustomHeaders: map[string]string{
			"Authorization": "Basic " + base64.StdEncoding.EncodeToString([]byte("foo"+":"+"qDDZdeQ5vw9XXFeVnXT4PZ--tGN2xNjjR4nrtyszZx0=")),
		},
	})
	var blockHash string
	err := rpcClient.CallFor(&blockHash, "getblockhash", height)
	if err != nil {
		log.Fatal("Error", err)
	}
	log.Println("Block hash:", blockHash)

	var blockInfo *BlockInfoResponse
	err = rpcClient.CallFor(&blockInfo, "getblock", blockHash)
	if err != nil {
		log.Fatal("Error", err)
	}
	log.Println("Block info:", blockInfo)

	var i = 0
	for i < blockInfo.TxsNumber {
		log.Println("TX:", blockInfo.Txs[i])
		var tx interface{}
		err = rpcClient.CallFor(&tx, "gettransaction", blockInfo.Txs[i])
		if err != nil {
			log.Fatal("Error", err)
		}
		log.Println("Tx:", tx)
		i++
	}

}

type TxTiny struct {
	Hash    string
	Address string
}

func fillBlocksAndValues() {
	blocksDb, err := leveldb.OpenFile("blocks.dat", nil)
	if err != nil {
		log.Fatalln(err)
	}
	valuesDb, err := leveldb.OpenFile("values.dat", nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer blocksDb.Close()
	defer valuesDb.Close()

	var blockNumber = 1
	var txNumber = 1
	for blockNumber <= 1000000 {

		var j = 0

		var nw = rand.Intn(10) // number of wallets involved
		if nw < 4 {
			nw = 0 // 50% chance it is 0
		} else {
			nw = nw - 3
		}

		for j < nw {
			var walletNumber = strconv.Itoa(rand.Intn(1000000))
			var key = cs(strconv.Itoa(blockNumber), walletNumber)
			var txsInBlock = rand.Intn(30) // number of txs in a block
			var txs = []string{}
			var l = 1

			for l < txsInBlock {
				txHash := "txhash-" + strconv.Itoa(txNumber)
				txs = append(txs, txHash)

				var voutsInTx = 1 + rand.Intn(2)
				var k = 1
				for k < voutsInTx {
					var voutKey = cs(txHash, strconv.Itoa(k))
					var voutData = ""
					switch purpose := rand.Intn(2); purpose {
					case 2:
						voutData = cs(cs(cs("empty", walletNumber), strconv.Itoa(rand.Intn(10))), "player-deposit")
					case 1:
						voutData = cs(cs(cs(walletNumber, "empty"), strconv.Itoa(rand.Intn(10))), "player-withdrawal")
					case 0:
						voutData = cs(cs(cs(walletNumber, walletNumber), strconv.Itoa(rand.Intn(10))), "change")
					}

					err = valuesDb.Put([]byte(voutKey), []byte(voutData), nil)
					if err != nil {
						log.Fatal("LevelDb error:", err)
					} else {
						log.Println("Filled ", voutKey, voutData)
					}
					k++
				}

				txNumber++
				l++
			}
			err = blocksDb.Put([]byte(key), []byte(ats(txs)), nil)
			if err != nil {
				log.Fatal("LevelDb error:", err)
			} else {
				log.Println("Filled ", key, txs)
			}
			j++
		}
		blockNumber++
	}
}

func fillBalCalcOptimized() {
	txDb, err := leveldb.OpenFile("tx.dat", nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer txDb.Close()

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

				err = txDb.Put([]byte(voutKey), []byte(voutData), nil)
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
}

func fetchTheLevel() {
	db, _ := leveldb.OpenFile("index.dat", nil)
	defer db.Close()

	data, err := db.Get([]byte(string(1000)), nil)
	if err != nil {
		log.Fatal("LevelDb error:", err)
	} else {
		log.Println(data)
	}
}

func countinLevel() {
	db, _ := leveldb.OpenFile("blocks.dat", nil)
	defer db.Close()
	iter := db.NewIterator(nil, nil)
	var i = 0
	wallets := make(map[string]int)
	max := 0
	maxWalletId := ""

	for iter.Next() {
		i++
		key := strings.Split(string(iter.Key()), "_")
		wallets[key[1]]++

		if wallets[key[1]] > max {
			max = wallets[key[1]]
			maxWalletId = key[1]
		}
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("LevelDb count:", i)
	log.Println("Wallets count:", len(wallets))
	log.Println("Biggest wallet:", maxWalletId, max)
}

func balance() {
	blocksDb, err := leveldb.OpenFile("blocks.dat", nil)
	if err != nil {
		log.Fatalln(err)
	}
	valuesDb, err := leveldb.OpenFile("values.dat", nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer blocksDb.Close()
	defer valuesDb.Close()

	wallets := make(map[string]int64)
	iter := blocksDb.NewIterator(&util.Range{Start: []byte("391182_"), Limit: []byte("446032_")}, nil)
	var txIndex int
	// iter := blocksDb.NewIterator(nil, nil)
	for iter.Next() {
		key := strings.Split(string(iter.Key()), "_")
		txs := strings.Split(string(iter.Value()), "_")
		// log.Println("Block:", string(iter.Key()), string(iter.Value()))
		for _, tx := range txs {
			// log.Println("TX:", tx)
			voutsIterator := valuesDb.NewIterator(util.BytesPrefix([]byte(tx+"_")), nil)
			for voutsIterator.Next() {
				vout := strings.Split(string(voutsIterator.Value()), "_")
				value, _ := strconv.ParseInt(vout[2], 10, 64)
				// log.Println(vout)
				switch purpose := vout[3]; purpose {
				case "player-withdrawal":
					wallets[key[1]] -= value
				case "player-deposit":
					wallets[key[1]] += value
				}
				txIndex++
			}

			voutsIterator.Release()
			err = iter.Error()
			if err != nil {
				log.Fatalln(err)
			}
		}
		// wallets[key[1]] = append(wallets[key[1]], strings.Split(string(iter.Value()), "_")...)
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
	log.Println("TX count:", txIndex)
	log.Println("Balance:", strconv.Itoa(int(total)))
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
