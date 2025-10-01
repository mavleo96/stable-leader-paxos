package database

import (
	"errors"
	"strconv"

	"github.com/mavleo96/cft-mavleo96/internal/models"
	"go.etcd.io/bbolt"
)

// InitDB initializes the database with "balances" bucket and adds accounts
// for the given account IDs, setting their initial balance to 10.
func InitDB(db *bbolt.DB, accountIds []string) error {
	return db.Update(func(tx *bbolt.Tx) error {
		// Create the "balances" bucket
		_, err := tx.CreateBucketIfNotExists([]byte("balances"))
		if err != nil {
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
}

// UpdateDB processes a transaction by updating the balances of the sender and receiver.
// It returns true if the transaction was successful, or false if the sender had insufficient funds.
func UpdateDB(db *bbolt.DB, t models.Transaction) (bool, error) {
	var success bool
	err := db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("balances"))
		// Retrieve sender and receiver balances
		sender_bal_bytes := b.Get([]byte(t.Sender))
		if sender_bal_bytes == nil {
			return errors.New("sender doesn't exist")
		}
		receiver_bal_bytes := b.Get([]byte(t.Receiver))
		if receiver_bal_bytes == nil {
			return errors.New("receiver doesn't exist")
		}
		sender_bal, err := strconv.Atoi(string(sender_bal_bytes))
		if err != nil {
			return err
		}
		receiver_bal, err := strconv.Atoi(string(receiver_bal_bytes))
		if err != nil {
			return err
		}

		// Check if sender has sufficient balance
		if sender_bal < t.Amount {
			success = false
			return nil
		}

		// Update balances
		err = b.Put([]byte(t.Sender), []byte(strconv.Itoa(sender_bal-t.Amount)))
		if err != nil {
			return err
		}
		err = b.Put([]byte(t.Receiver), []byte(strconv.Itoa(receiver_bal+t.Amount)))
		if err != nil {
			return err
		}
		success = true
		return nil
	})
	return success, err
}
