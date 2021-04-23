package main

import (
	"context"
	"fmt"
	"github.com/NebulousLabs/go-skynet/v2"
	"github.com/bluemediaapp/models"
	"github.com/gofiber/fiber/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"strconv"
)

var (
	app    = fiber.New()
	client *mongo.Client
	config *Config

	mctx = context.Background()

	videosCollection *mongo.Collection

	cachedVideos = make([]int64, 0, 0)
)

func main() {
	max, err := strconv.ParseInt(os.Getenv("max_cached_videos"), 10, 32)
	if err != nil {
		max = 10
	}
	config = &Config{
		port:            os.Getenv("port"),
		mongoUri:        os.Getenv("mongo_uri"),
		maxCachedVideos: int(max),
	}
	skyClient := skynet.New()

	app.Get("/videos/:video_id", func(ctx *fiber.Ctx) error {
		videoId, err := strconv.ParseInt(ctx.Params("video_id"), 10, 64)
		if err != nil {
			return err
		}
		if contains(cachedVideos, videoId) {
			return ctx.SendFile(fmt.Sprint("cached/", videoId), true)
		}

		video, err := getVideo(videoId)
		if err != nil {
			return ctx.Status(404).SendString("Video not found.")
		}

		err = skyClient.DownloadFile(fmt.Sprint("./cached/", videoId), video.StorageKey, skynet.DefaultDownloadOptions)
		if err != nil {
			return err
		}

		if len(cachedVideos) > config.maxCachedVideos {
			oldVideoId, cached := cachedVideos[0], cachedVideos[1:]
			cachedVideos = cached
			err := os.Remove(fmt.Sprint("cached/", oldVideoId))
			if err != nil {
				return err
			}
		}

		cachedVideos = append(cachedVideos, videoId)

		return ctx.SendFile(fmt.Sprint("./cached/", videoId))
	})

	initDb()
	log.Fatal(app.Listen(config.port))
}

func initDb() {
	// Connect mongo
	var err error
	client, err = mongo.NewClient(options.Client().ApplyURI(config.mongoUri))
	if err != nil {
		log.Fatal(err)
	}

	err = client.Connect(mctx)
	if err != nil {
		log.Fatal(err)
	}

	// Setup tables
	db := client.Database("blue")
	videosCollection = db.Collection("video_metadata")
}

func contains(slice []int64, val int64) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

func getVideo(videoId int64) (models.DatabaseVideo, error) {
	query := bson.D{{"_id", videoId}}
	rawVideo := videosCollection.FindOne(mctx, query)
	var video models.DatabaseVideo
	err := rawVideo.Decode(&video)
	if err != nil {
		return models.DatabaseVideo{}, err
	}
	return video, nil
}
