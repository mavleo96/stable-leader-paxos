package database

import (
	"strconv"

	pb "github.com/mavleo96/stable-leader-paxos/pb"
	"go.etcd.io/bbolt"
)

type Database struct {
	db *bbolt.DB
}

// InitDB initializes the database with "balances" bucket and adds accounts
// for the given account IDs, setting their initial balance to 10.
func (d *Database) InitDB(dbPath string, accountIds []string) (err error) {
	boltDB, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		return err
	}
	d.db = boltDB

	err = d.db.Update(func(tx *bbolt.Tx) error {
		// Create the "balances" bucket
		_, err := tx.CreateBucket([]byte("balances"))
		if err != nil {
			d.db.Close()
			return err
		}

		// Add each account with an initial balance of 10
		b := tx.Bucket([]byte("balances"))
		for _, id := range accountIds {
			if err := b.Put([]byte(id), []byte("10")); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// UpdateDB processes a transaction by updating the balances of the sender and receiver.
// It returns true if the transaction was successful, or false if the sender had insufficient funds.
func (d *Database) UpdateDB(t *pb.Transaction) (bool, error) {
	var success bool
	err := d.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("balances"))

		// Retrieve sender and receiver balances
		senderBalBytes := b.Get([]byte(t.Sender))
		if senderBalBytes == nil {
			success = false
			return nil
		}
		receiverBalBytes := b.Get([]byte(t.Receiver))
		if receiverBalBytes == nil {
			success = false
			return nil
		}
		senderBal, err := strconv.Atoi(string(senderBalBytes))
		if err != nil {
			return err
		}
		receiverBal, err := strconv.Atoi(string(receiverBalBytes))
		if err != nil {
			return err
		}

		// Check if amount is valid and sender has sufficient balance
		amount := int(t.Amount)
		if amount <= 0 {
			success = false
			return nil
		}
		if senderBal < amount {
			success = false
			return nil
		}

		// Update balances
		err = b.Put([]byte(t.Sender), []byte(strconv.Itoa(senderBal-amount)))
		if err != nil {
			return err
		}
		err = b.Put([]byte(t.Receiver), []byte(strconv.Itoa(receiverBal+amount)))
		if err != nil {
			return err
		}
		success = true
		return nil
	})
	return success, err
}

func (d *Database) PrintDB() (map[string]int, error) {
	db_state := make(map[string]int)
	err := d.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("balances"))
		b.ForEach(func(k, v []byte) error {
			n, err := strconv.Atoi(string(v))
			if err != nil {
				return err
			}
			db_state[string(k)] = n
			return nil
		})
		return nil
	})
	return db_state, err
}

// Close closes the database
func (d *Database) Close() error {
	return d.db.Close()
}
