package main

import (
	"os"
	"strconv"
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

// +gen slice:"Select[string],DistinctBy"
type Chunk struct {
	ID      string    `bson:"_id"`
	Ns      string    `bson:"ns"`
	Shard   string    `bson:"shard"`
	Jumbo   bool      `bson:"jumbo"`
	Lastmod time.Time `bson:"lastmod"`
}

type CollectionInfo struct {
	chunksNum      int
	jumboChunksNum int
	shardChunks    map[string]int
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

	// グローバルオプション設定
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
		config := session.DB("config")
		// db := session.DB(database)

		// shards := getShards(config)
		chunks := getChunks(config)

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
		collections := chunks.DistinctBy(func(arg1 Chunk, arg2 Chunk) bool {
			return arg1.Ns == arg2.Ns
		}).SelectString(func(arg1 Chunk) string {
			return arg1.Ns
		})

		// collectionInfo init
		collectionInfos := make(map[string]map[string]int, len(collections))
		for i := 0; i < len(collections); i++ {
			collectionInfos[collections[i]] = map[string]int{
				"chunksNum":      0,
				"jumboChunksNum": 0,
			}
		}

		// fmt.Println(collectionInfos["abema.devices"])

		for i := 0; i < len(chunks); i++ {
			collectionInfos[chunks[i].Ns]["chunksNum"]++
			if chunks[i].Jumbo {
				collectionInfos[chunks[i].Ns]["jumboChunksNum"]++
			}
		}

		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Name", "Chunks", "JumboChunks"})
		// table.SetBorders(tablewriter.Border{Left: true, Top: true, Right: true, Bottom: false})
		// table.SetBorder(false)
		// table.AppendBulk(data) // Add Bulk Data

		for name, info := range collectionInfos {
			table.Append([]string{
				name,
				strconv.Itoa(info["chunksNum"]),
				strconv.Itoa(info["jumboChunksNum"]),
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

func getChunks(db *mgo.Database) ChunkSlice {
	var chunks ChunkSlice
	err := db.C("chunks").Find(bson.M{}).All(&chunks)
	if err != nil {
		panic(err)
	}
	return chunks
}
