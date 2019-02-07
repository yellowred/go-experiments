/**
 * DynamoDb test
 *
**/

package main

import (
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"

	// "time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func createTables() {

	config := &aws.Config{
		Region:   aws.String("us-west-2"),
		Endpoint: aws.String("http://localhost:8000"),
	}

	sess := session.Must(session.NewSession(config))

	svc := dynamodb.New(sess)

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("block_height"),
				AttributeType: aws.String("N"),
			},
			{
				AttributeName: aws.String("transaction_id"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("block_height"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("transaction_id"),
				KeyType:       aws.String("RANGE"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1000),
			WriteCapacityUnits: aws.Int64(1000),
		},
		TableName: aws.String("transactions"),
	}

	result, err := svc.CreateTable(input)
	if err != nil {
		log.Println(err.Error())
		return
	}

	log.Println(result)
}

type Transaction struct {
	BlockHeight   int    `json:"block_height"`
	TransactionID string `json:"transaction_id"`
	FromWallet    string `json:"from_wallet"`
	ToWallet      string `json:"to_wallet"`
	Value         int    `json:"value"`
	Purpose       string `json:"purpose"`
}

const xthreads = 1000

func put(batchItemsArray []*dynamodb.WriteRequest, svc *dynamodb.DynamoDB) {
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			"transactions": batchItemsArray,
		},
	}

	result, err := svc.BatchWriteItem(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeProvisionedThroughputExceededException:
				log.Println(dynamodb.ErrCodeProvisionedThroughputExceededException, aerr.Error())
			case dynamodb.ErrCodeResourceNotFoundException:
				log.Println(dynamodb.ErrCodeResourceNotFoundException, aerr.Error())
			case dynamodb.ErrCodeItemCollectionSizeLimitExceededException:
				log.Println(dynamodb.ErrCodeItemCollectionSizeLimitExceededException, aerr.Error())
			case dynamodb.ErrCodeRequestLimitExceeded:
				log.Println(dynamodb.ErrCodeRequestLimitExceeded, aerr.Error())
			case dynamodb.ErrCodeInternalServerError:
				log.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
			default:
				log.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			log.Println(err.Error())
		}
	}

	log.Println(result)
}

func fillBalCalcOptimized() {
	config := &aws.Config{
		Region:   aws.String("us-west-2"),
		Endpoint: aws.String("http://localhost:8000"),
	}

	sess := session.Must(session.NewSession(config))

	svc := dynamodb.New(sess)

	var blockNumber = 1
	var txNumber = 1
	var total = 0

	var batchCount = 0
	batchItems := make([]*dynamodb.WriteRequest, 25)

	// start workers
	messages := make(chan []*dynamodb.WriteRequest)
	var wg sync.WaitGroup

	// This starts xthreads number of goroutines that wait for something to do
	wg.Add(xthreads)
	for i := 0; i < xthreads; i++ {
		go func() {
			for {
				a, ok := <-messages
				if !ok { // if there is nothing to do and the channel has been closed then end the goroutine
					wg.Done()
					return
				}
				put(a, svc) // do the thing
			}
		}()
	}

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
				var walletNumber = strconv.Itoa(rand.Intn(1000000))
				var transactionID = cs(txHash, strconv.Itoa(k))
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

				tx := Transaction{
					BlockHeight:   blockNumber,
					TransactionID: transactionID,
					FromWallet:    fromWalletHash,
					ToWallet:      toWalletHash,
					Value:         rand.Intn(10),
					Purpose:       purposeLabel,
				}

				av, err := dynamodbattribute.MarshalMap(tx)
				if err != nil {
					log.Fatalln(err)
				}

				if batchCount < 25 {
					batchItems[batchCount] = &dynamodb.WriteRequest{
						PutRequest: &dynamodb.PutRequest{Item: av},
					}
					batchCount++
				} else {
					messages <- batchItems[0 : batchCount-1]
					batchCount = 0
				}
				total++
				k++
			}
			l++
			txNumber++
		}
		blockNumber++
	}

	wg.Wait()
	log.Println(blockNumber, txNumber, total)
}

// func balanceForOptimized(txDb *sql.DB) {

// 	start := time.Now()

// 	var txIndex int
// 	// wallets := make(map[string]int64)
// 	queryStmt, err := txDb.Prepare("SELECT block_height, transaction_hash, identifier, purpose, value FROM auditor_txs WHERE block_height >= $1 AND block_height <= $2;")
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	rows, err := queryStmt.Query(391182, 446032)
// 	defer rows.Close()
// 	var total int

// 	for rows.Next() {
// 		var blockHeight int
// 		var txHash string
// 		var identifier int
// 		var purpose string
// 		var amount int

// 		if err := rows.Scan(&blockHeight, &txHash, &identifier, &purpose, &amount); err != nil {
// 			log.Fatal(err)
// 		}
// 		// log.Println(txHash)
// 		// amount, _ := strconv.ParseInt(row[4], 10, 64)
// 		switch purpose {
// 		case "player-withdrawal":
// 			total -= amount
// 		case "player-deposit":
// 			total += amount
// 		}
// 		txIndex++
// 	}

// 	elapsed := time.Since(start)
// 	log.Printf("Balance took %s", elapsed)

// 	// log.Println("Wallets count:", len(wallets))
// 	log.Println("VOuts count:", txIndex)
// 	log.Println("Balance:", strconv.Itoa(int(total)))
// }

func ats(arr []string) string {
	return strings.Join(arr, "_")
}

func cs(a string, b string) string {
	return strings.Join([]string{a, b}, "_")
}

func main() {
	if len(os.Args) > 1 {
		if os.Args[1] == "create" {
			createTables()
		} else if os.Args[1] == "fill" {
			fillBalCalcOptimized()
		} else if os.Args[1] == "query" {
			// balanceForOptimized(txDb)
		}
	}
}
