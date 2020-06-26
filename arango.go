package arangorm

import (
	"context"
	"encoding/json"
	"fmt"

	driver "github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
)

type Instance struct {
	DB    driver.Database
	graph string
}

type Config struct {
	Hosts []string
	User  string
	Pass  string
	Db    string
}

func New(config Config, graphName string) (*Instance, error) {
	var db driver.Database

	// Open the connection to the Arango sever
	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: config.Hosts,
		//TLSConfig:          nil,
		//Transport:          nil,
		//DontFollowRedirect: false,
		//FailOnRedirect:     false,
		//ConnectionConfig:   cluster.ConnectionConfig{},
		//ContentType:        0,
		//ConnLimit:          0,
	})
	if err != nil {
		return nil, err
	}

	// CreateVertex an Arango client to perform the operations.
	client, err := driver.NewClient(driver.ClientConfig{
		Connection:     conn,
		Authentication: driver.BasicAuthentication(config.User, config.Pass),
		//SynchronizeEndpointsInterval: 0,
	})
	if err != nil {
		return nil, err
	}

	// If the database requested doesnt exits? create it!
	found, err := client.DatabaseExists(context.Background(), config.Db)
	if err != nil {
		return nil, err
	}
	if !found {
		options := driver.CreateDatabaseOptions{
			Users: []driver.CreateDatabaseUserOptions{
				{
					UserName: config.User,
					Password: config.Pass,
				},
			},
			Options: driver.CreateDatabaseDefaultOptions{},
		}

		db, err = client.CreateDatabase(context.Background(), config.Db, &options)
		if err != nil {
			return nil, err
		}
	} else {
		// Open a database connection to return it.
		db, err = client.Database(context.Background(), config.Db)
		if err != nil {
			return nil, err
		}
	}

	return &Instance{db, graphName}, nil
}

func (app *Instance) LoadGraph(options driver.CreateGraphOptions) error {
	found, err := app.DB.GraphExists(context.Background(), app.graph)
	if err != nil {
		return err
	}

	if !found {
		if _, err := app.DB.CreateGraph(context.Background(), app.graph, &options); err != nil {
			return err
		}
	}
	return nil
}

// DATABASE HELPERS
func (app *Instance) Execute(query string, bindVars map[string]interface{}, result interface{}) error {
	ctx := context.Background()
	cursor, err := app.DB.Query(ctx, query, bindVars)
	if err != nil {
		return err
	}
	defer cursor.Close()

	collection := []byte("[")

	for {
		var element interface{}

		if _, err := cursor.ReadDocument(ctx, &element); err != nil {
			if driver.IsNoMoreDocuments(err) {
				break
			} else {
				return err
			}
		}

		if len(collection) > 1 {
			collection = append(collection, ',')
		}

		bytes, err := json.Marshal(element)
		if err != nil {
			return err
		}

		collection = append(collection, bytes...)
	}

	collection = append(collection, ']')

	return json.Unmarshal(collection, result)
}

func (app *Instance) FetchVertex(collection, key string, result interface{}) (driver.DocumentMeta, error) {
	graph, err := app.DB.Graph(context.Background(), app.graph)
	if err != nil {
		return driver.DocumentMeta{}, err
	}

	col, err := graph.VertexCollection(context.Background(), collection)
	if err != nil {
		return driver.DocumentMeta{}, err
	}

	return col.ReadDocument(context.Background(), key, result)
}

func (app *Instance) FetchEdge(collection, key string, result interface{}) (driver.DocumentMeta, error) {
	graph, err := app.DB.Graph(context.Background(), app.graph)
	if err != nil {
		return driver.DocumentMeta{}, err
	}

	col, _, err := graph.EdgeCollection(context.Background(), collection)
	if err != nil {
		return driver.DocumentMeta{}, err
	}

	return col.ReadDocument(context.Background(), key, result)
}

func (app *Instance) CreateVertex(collection string, element interface{}) (driver.DocumentMeta, error) {
	graph, err := app.DB.Graph(context.Background(), app.graph)
	if err != nil {
		return driver.DocumentMeta{}, err
	}

	col, err := graph.VertexCollection(context.Background(), collection)
	if err != nil {
		return driver.DocumentMeta{}, err
	}

	return col.CreateDocument(context.Background(), element)
}

func (app *Instance) CreateEdge(collection string, element interface{}) (driver.DocumentMeta, error) {
	graph, err := app.DB.Graph(context.Background(), app.graph)
	if err != nil {
		return driver.DocumentMeta{}, err
	}

	col, _, err := graph.EdgeCollection(context.Background(), collection)
	if err != nil {
		return driver.DocumentMeta{}, err
	}

	return col.CreateDocument(context.Background(), element)
}

func (app *Instance) CreateEdges(collection string, elements interface{}) (driver.DocumentMetaSlice, driver.ErrorSlice, error) {
	graph, err := app.DB.Graph(context.Background(), app.graph)
	if err != nil {
		return nil, nil, err
	}

	col, _, err := graph.EdgeCollection(context.Background(), collection)
	if err != nil {
		return nil, nil, err
	}

	return col.CreateDocuments(context.Background(), elements)
}

func (app *Instance) UpdateVertex(collection string, key string, element interface{}) (driver.DocumentMeta, error) {
	graph, err := app.DB.Graph(context.Background(), app.graph)
	if err != nil {
		return driver.DocumentMeta{}, err
	}

	col, err := graph.VertexCollection(context.Background(), collection)
	if err != nil {
		return driver.DocumentMeta{}, err
	}

	return col.UpdateDocument(context.Background(), key, element)
}

func (app *Instance) UpdateEdge(collection string, key string, element interface{}) (driver.DocumentMeta, error) {
	graph, err := app.DB.Graph(context.Background(), app.graph)
	if err != nil {
		return driver.DocumentMeta{}, err
	}

	col, _, err := graph.EdgeCollection(context.Background(), collection)
	if err != nil {
		return driver.DocumentMeta{}, err
	}

	return col.UpdateDocument(context.Background(), key, element)
}

func (app *Instance) RemoveVertex(collection string, key string) (driver.DocumentMeta, error) {
	graph, err := app.DB.Graph(context.Background(), app.graph)
	if err != nil {
		return driver.DocumentMeta{}, err
	}

	col, err := graph.VertexCollection(context.Background(), collection)
	if err != nil {
		return driver.DocumentMeta{}, err
	}

	return col.RemoveDocument(context.Background(), key)
}

func (app *Instance) RemoveEdge(collection string, key string) (driver.DocumentMeta, error) {
	graph, err := app.DB.Graph(context.Background(), app.graph)
	if err != nil {
		return driver.DocumentMeta{}, err
	}

	col, _, err := graph.EdgeCollection(context.Background(), collection)
	if err != nil {
		return driver.DocumentMeta{}, err
	}

	return col.RemoveDocument(context.Background(), key)
}

// UTILS
func GetId(collection, key string) driver.DocumentID {
	return driver.DocumentID(fmt.Sprintf("%s/%s", collection, key))
}
