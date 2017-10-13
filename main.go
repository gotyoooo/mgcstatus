package main

import (
	"math"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/codegangsta/cli"
	"github.com/olekukonko/tablewriter"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// +gen slice:""
type Shard struct {
	ID   string `bson:"_id"`
	Host string `bson:"host"`
}

// +gen slice:"Where"
type Chunk struct {
	ID    string `bson:"_id"`
	Ns    string `bson:"ns"`
	Shard string `bson:"shard"`
	Jumbo bool   `bson:"jumbo"`
}

// +gen slice:"Where"
type Collection struct {
	ID        string `bson:"_id"`
	NoBalance bool   `bson:"noBalance"`
}

type Collstats struct {
	Ns         string  `bson:"ns"`
	Count      int     `bson:"count"`
	AvgObjSize float64 `bson:"avgObjSize"`
}

func main() {
	app := cli.NewApp()
	app.Name = "mgcstatus"
	app.Usage = "Get the status of chunk for each collection of sharding mongodb cluster"
	app.Version = "0.0.1"

	var host string
	var port int
	var database string

	// Global Option
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "host",
			Value:       "localhost",
			Usage:       "server to connect to",
			Destination: &host,
		},
		cli.IntFlag{
			Name:        "port",
			Value:       27017,
			Usage:       "port to connect to",
			Destination: &port,
		},
		cli.StringFlag{
			Name:        "db, d",
			Value:       "test",
			Usage:       "database to check status",
			Destination: &database,
		},
	}

	app.Action = func(c *cli.Context) error {
		// init mongodb client
		session := getConnection(host, port)
		defer session.Close()
		configDb := session.DB("config")
		selectDb := session.DB(database)

		// get config status
		cfShards := getShards(configDb)
		cfChunks := getChunks(configDb, database)
		cfCollections := getCollections(configDb, database)
		shardsNum := len(cfShards)
		collectionsNum := len(cfCollections)

		// collections sort by name
		sort.Slice(cfCollections, func(i int, j int) bool {
			return cfCollections[i].ID < cfCollections[j].ID
		})

		// Table setting
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{
			"CollectionName",
			"Objs",
			"chunks",
			"aveChunkSize(KB)",
			"idealChunksPerShards",
			"remainChunks",
			"remainChunksSize(KB)",
			"Jumbos",
			"balancer",
		})

		// create collection info
		for i := 0; i < collectionsNum; i++ {
			collectionName := cfCollections[i].ID
			collectionNameWithoutDb := strings.Split(collectionName, ".")[1]

			// get data
			chunks := cfChunks.Where(func(arg1 Chunk) bool {
				return arg1.Ns == collectionName
			})
			chunksNum := len(chunks)
			colstats := getCollStats(selectDb, collectionNameWithoutDb)
			aveObjSize := colstats.AvgObjSize
			objsNum := colstats.Count
			jumboChunksNum := len(chunks.Where(func(arg1 Chunk) bool {
				return arg1.Jumbo == true
			}))
			aveChunkSize := objsNum / chunksNum * int(aveObjSize)

			// check ideal per shard
			idealChunksPerShardsNum := 1
			if chunksNum > shardsNum {
				idealChunksPerShardsNum = int(math.Ceil(float64(chunksNum) / float64(shardsNum)))
			}

			// get remain chunks data
			remainChunksNum := 0
			for j := 0; j < shardsNum; j++ {
				shardChunksNum := len(chunks.Where(func(arg1 Chunk) bool {
					return arg1.Shard == cfShards[j].ID
				}))
				if shardChunksNum > idealChunksPerShardsNum {
					remainChunksNum += (shardChunksNum - idealChunksPerShardsNum)
				}
			}
			remainChunksSize := aveChunkSize * remainChunksNum

			// check balancer status
			balancer := 1
			if cfCollections[i].NoBalance {
				balancer = 0
			}

			// add table row
			table.Append([]string{
				collectionName,
				strconv.Itoa(chunksNum),
				strconv.Itoa(objsNum),
				strconv.FormatFloat((float64(aveChunkSize) / float64(1024)), 'f', 2, 64),
				strconv.Itoa(idealChunksPerShardsNum),
				strconv.Itoa(remainChunksNum),
				strconv.FormatFloat((float64(remainChunksSize) / float64(1024)), 'f', 2, 64),
				strconv.Itoa(jumboChunksNum),
				strconv.Itoa(balancer),
			})
		}

		// Table Output
		table.Render()

		return nil
	}

	app.Run(os.Args)
}

func getConnection(host string, port int) *mgo.Session {
	session, err := mgo.Dial("mongodb://" + host + ":" + strconv.Itoa(port))
	if err != nil {
		panic(err)
	}
	return session
}

func getShards(db *mgo.Database) ShardSlice {
	var shards ShardSlice
	err := db.C("shards").Find(bson.M{}).All(&shards)
	if err != nil {
		panic(err)
	}
	return shards
}

func getChunks(db *mgo.Database, database string) ChunkSlice {
	var chunks ChunkSlice
	err := db.C("chunks").Find(bson.M{}).All(&chunks)
	if err != nil {
		panic(err)
	}
	return chunks.Where(func(arg1 Chunk) bool {
		return strings.Split(arg1.Ns, ".")[0] == database
	})
}

func getCollections(db *mgo.Database, database string) CollectionSlice {
	var collections CollectionSlice
	err := db.C("collections").Find(bson.M{}).All(&collections)
	if err != nil {
		panic(err)
	}
	return collections.Where(func(arg1 Collection) bool {
		return strings.Split(arg1.ID, ".")[0] == database
	})
}

func getCollStats(db *mgo.Database, collection string) Collstats {
	var collStats Collstats
	err := db.Run(bson.M{"collStats": collection}, &collStats)
	if err != nil {
		panic(err)
	}
	return collStats
}
