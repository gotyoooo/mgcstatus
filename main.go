package main

import (
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/codegangsta/cli"
	"github.com/olekukonko/tablewriter"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// +gen slice:"Select[string]"
type Shard struct {
	ID   string `bson:"_id"`
	Host string `bson:"host"`
}

// +gen slice:"Select[string],DistinctBy,Where"
type Chunk struct {
	ID      string    `bson:"_id"`
	Ns      string    `bson:"ns"`
	Shard   string    `bson:"shard"`
	Jumbo   bool      `bson:"jumbo"`
	Lastmod time.Time `bson:"lastmod"`
}

// +gen slice:"Where"
type Collection struct {
	ID        string `bson:"_id"`
	NoBalance bool   `bson:"noBalance"`
}

type Collstats struct {
	Ns         string  `bson:"ns"`
	AvgObjSize float64 `bson:"avgObjSize"`
}

// func main() {
//
// }

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
		session := getConnection(host, port)
		defer session.Close()
		configDb := session.DB("config")
		selectDb := session.DB(database)

		shards := getShards(configDb)
		shardsNum := len(shards)
		// cfChunks := getChunks(configDb, database)
		cfCollections := getCollections(configDb, database)

		// fmt.Println("Results All: ", shards)
		// for key, shard := range shards {
		// 	fmt.Println("key:", key, " ID:", shard.ID, " host:", shard.Host)
		// }
		// for key, chunk := range chunks {
		// 	// if !  {
		// 	//
		// 	// }
		// 	fmt.Println("key:", key, " ID:", chunk.Ns, " NS:", chunk.Ns, " Shard:", chunk.Shard)
		// }
		// shardIds := shards.SelectString(func(arg1 Shard) string {
		// 	return arg1.ID
		// })
		// collections := cfChunks.DistinctBy(func(arg1 Chunk, arg2 Chunk) bool {
		// 	return arg1.Ns == arg2.Ns
		// }).SelectString(func(arg1 Chunk) string {
		// 	return arg1.Ns
		// })

		// create collection info
		collectionInfos := make(map[string]map[string]int, len(cfCollections))
		for i := 0; i < len(cfCollections); i++ {
			collectionName := cfCollections[i].ID
			collectionNameWithoutDb := strings.Split(collectionName, ".")[1]

			// get data
			chunksNum := getFindCount(configDb, bson.M{"ns": collectionName}, "chunks")
			aveObjSize := getCollStats(selectDb, collectionNameWithoutDb).AvgObjSize
			objsNum := getCount(selectDb, collectionNameWithoutDb)
			jumboChunksNum := getFindCount(configDb, bson.M{"ns": collectionName, "jumbo": true}, "chunks")

			// check ideal per shard
			idealChunksPerShardsNum := 1
			if chunksNum > shardsNum {
				idealChunksPerShardsNum = int(math.Ceil(float64(chunksNum) / float64(shardsNum)))
			}

			// check balancer status
			balancer := 1
			if cfCollections[i].NoBalance {
				balancer = 0
			}

			collectionInfos[collectionName] = map[string]int{
				"chunksNum":               chunksNum,
				"objsNum":                 objsNum,
				"aveChunkSize":            objsNum / chunksNum * int(aveObjSize),
				"idealChunksPerShardsNum": idealChunksPerShardsNum,
				"jumboChunksNum":          jumboChunksNum,
				"balancerStatus":          balancer,
			}
		}

		// fmt.Println(collectionInfos)
		// count chunks num
		// for i := 0; i < len(cfChunks); i++ {
		// 	collectionInfos[cfChunks[i].Ns]["chunksNum"]++
		// 	if cfChunks[i].Jumbo {
		// 		collectionInfos[cfChunks[i].Ns]["jumboChunksNum"]++
		// 	}
		// }

		// get collection infomation from database
		// for i := 0; i < len(cfCollections); i++ {
		// 	collectionName := strings.Split(cfCollections[i].ID, ".")[1]
		// 	// docs := getCount(selectDb, collectionName)
		// 	// fmt.Println(cfCollections[i].ID, docs)
		// 	collectionInfos[cfCollections[i].ID] = merge(
		// 		collectionInfos[cfCollections[i].ID],
		// 		map[string]int{
		// 			"docsNum": getCount(selectDb, collectionName),
		// 		},
		// 	)
		// }

		// Table Output
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Name", "Objs", "chunks", "aveChunkSize", "idealChunksPerShards", "Jumbos", "balancer"})
		// table.SetBorders(tablewriter.Border{Left: true, Top: true, Right: true, Bottom: false})
		// table.SetBorder(false)
		// table.AppendBulk(data) // Add Bulk Data

		for name, info := range collectionInfos {
			table.Append([]string{
				name,
				strconv.Itoa(info["objsNum"]),
				strconv.Itoa(info["chunksNum"]),
				strconv.Itoa(info["aveChunkSize"]),
				strconv.Itoa(info["idealChunksPerShardsNum"]),
				strconv.Itoa(info["jumboChunksNum"]),
				strconv.Itoa(info["balancerStatus"]),
			})
		}
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

func getFindCount(db *mgo.Database, query interface{}, collection string) int {
	cnt, err := db.C(collection).Find(query).Count()
	if err != nil {
		panic(err)
	}
	return cnt
}

func getCount(db *mgo.Database, collection string) int {
	cnt, err := db.C(collection).Count()
	if err != nil {
		panic(err)
	}
	return cnt
}

func getCollStats(db *mgo.Database, collection string) Collstats {
	var collStats Collstats
	err := db.Run(bson.M{"collStats": collection}, &collStats)
	if err != nil {
		panic(err)
	}
	return collStats
}

func merge(m1, m2 map[string]int) map[string]int {
	ans := map[string]int{}

	for k, v := range m1 {
		ans[k] = v
	}
	for k, v := range m2 {
		ans[k] = v
	}
	return (ans)
}
