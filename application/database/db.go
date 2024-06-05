package database

import (
	"context"
	"os"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Database struct
// This struct represents a database connection
// It has a pointer to a mongo.Database and the name of the database
// The [Db] is the pointer to the mongo.Database
// The [dbName] is the name of the database
type Database struct {
	Db     *mongo.Database
	dbName string
}

// NewDb function
// This function returns a new Database struct
// It receives the name of the database and returns a pointer to the Database struct
func NewDb(dbName string) *Database {
	return &Database{
		dbName: dbName,
	}
}

// GetCollection function
// This function returns a pointer to a mongo.Collection
// It receives the name of the collection and returns a pointer to the mongo.Collection
func (d *Database) GetCollection(collection string) *mongo.Collection {
	return d.Db.Collection(collection)
}

// Connect function
// This function connects to the database
// It returns a pointer to the mongo.Database and an error
func (d *Database) Connect() (*mongo.Database, error) {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(os.Getenv("MONGO_URL")))

	if err != nil {
		return nil, err
	}

	d.Db = client.Database(d.dbName)

	return d.Db, nil
}
