package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/robfig/cron/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type GraphData struct {
	Nodes []struct {
		ID                 string `json:"id"`
		AbsoluteImportance int    `json:"absolute_importance"`
		AbsoluteDependence int    `json:"absolute_dependence"`
	} `json:"nodes"`
	Edges []struct {
		Source      string  `json:"source"`
		Target      string  `json:"target"`
		Latency     float64 `json:"latency"`
		Frequency   int     `json:"frequency"`
		CoExecution float64 `json:"co_execution"`
	} `json:"edges"`
}

type WeightedGraphResponse struct {
	Status     string    `json:"status"`
	Message    string    `json:"message"`
	WeightType string    `json:"weight_type"`
	GapTime    string    `json:"gap_time"`
	Data       GraphData `json:"data"`
}

type Metrics struct {
	StartTime int64
	EndTime   int64
	Data      GraphData
}

type UpdateLog struct {
	LastFetchTime int64 `bson:"last_fetch_time"`
}

var MONGO_URI string

type Container struct {
	GET_WEIGHT_GRAPH_API string
	MONGO_DB             string
	MetricsCollection    string
	UpdateLogCollection  string
}

func ConnectToMongoDB() (*mongo.Client, error) {
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(MONGO_URI))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %v", err)
	}
	log.Println("Connected to MongoDB successfully.")
	return client, nil
}

func (c *Container) CalculateMetricAndPushToDB() {
	log.Printf("Running cron job Calculate And Push Metrics at %v...", time.Now().Format(time.RFC3339))

	end_time := time.Now().UnixMicro()
	start_time := end_time - (15 * time.Minute).Microseconds()
	log.Println("Setting Start time:", start_time, "End time:", end_time)

	client, err := ConnectToMongoDB()
	if err != nil {
		log.Fatal("Error connecting to MongoDB:", err)
		log.Println("Skipping this job..")
		return
	}
	defer client.Disconnect(context.TODO())
	updateLogCollectionObj := client.Database(c.MONGO_DB).Collection(c.UpdateLogCollection)

	query := bson.M{}
	opts := options.Find().SetSort(bson.D{{Key: "last_fetch_time", Value: -1}}).SetLimit(1)

	cursor, err := updateLogCollectionObj.Find(context.TODO(), query, opts)
	if err != nil {
		log.Printf("Error fetching last fetch time: %v\n", err)
		return
	}
	defer cursor.Close(context.TODO())

	if cursor.Next(context.TODO()) {
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			log.Printf("Error decoding fetch time: %v\n", err)
			return
		}
		log.Printf("Last fetch time found: %v and is before start_time: %t ", result["last_fetch_time"].(int64), result["last_fetch_time"].(int64) < start_time)
		start_time = result["last_fetch_time"].(int64)
		log.Printf("Setting start_time: %d, end_time: %d \n", start_time, end_time)
	} else {
		log.Println("No previous fetch time found, setting start_time to 15 minutes ago")
	}

	graph_data, err := c.CalculateMetric(start_time, end_time)
	if err != nil {
		log.Println("Error calculating metrics:", err)
		return
	}

	if len(graph_data.Nodes) == 0 || len(graph_data.Edges) == 0 {
		log.Println("Graph data is empty. Skipping push to MongoDB.")
		return
	}

	if err := c.PushToDB(Metrics{
		StartTime: start_time,
		EndTime:   end_time,
		Data:      graph_data,
	}); err != nil {
		log.Println("Error saving to MongoDB:", err)
	}
}

func (c *Container) CalculateMetric(start_time int64, end_time int64) (GraphData, error) {
	log.Println("Calculating metrics for time period:", start_time, end_time)
	url := fmt.Sprintf(c.GET_WEIGHT_GRAPH_API, start_time, end_time)

	resp, err := http.Get(url)
	if err != nil {
		return GraphData{}, fmt.Errorf("failed to get weighted graph for time period %v - %v Error: %v", start_time, end_time, err)
	}
	log.Println("Received response from API:", resp.Status)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return GraphData{}, fmt.Errorf("failed to read response body: %v", err)
	}
	log.Println("Response body:", string(body)[:10])

	var weightedGraphResp WeightedGraphResponse
	if err := json.Unmarshal(body, &weightedGraphResp); err != nil {
		return GraphData{}, fmt.Errorf("failed to unmarshal JSON: %v", err)
	}
	log.Println("Unmarshalled JSON with data:", weightedGraphResp.Data)

	return weightedGraphResp.Data, nil
}

func (c *Container) PushToDB(data Metrics) error {
	if data.Data.Nodes == nil || data.Data.Edges == nil {
		log.Println("Graph data is empty. Skipping push to MongoDB.")
		return nil
	}

	log.Println("Pushing graph to MongoDB...")

	client, err := ConnectToMongoDB()
	if err != nil {
		log.Fatal("Error connecting to MongoDB:", err)
		log.Println("Skipping this job..")
		return err
	}

	log.Println("Inserting graph to MongoDB...")
	metricsCollectionObj := client.Database(c.MONGO_DB).Collection(c.MetricsCollection)
	updateLogCollectionObj := client.Database(c.MONGO_DB).Collection(c.UpdateLogCollection)
	defer client.Disconnect(context.TODO())

	_, err = metricsCollectionObj.InsertOne(context.TODO(), data)
	if err != nil {
		return fmt.Errorf("failed to insert graph: %v", err)
	}
	log.Println("Graph saved to MongoDB successfully.")

	log.Printf("Updating last fetched time %v", data.EndTime)
	_, err = updateLogCollectionObj.InsertOne(context.TODO(), UpdateLog{LastFetchTime: data.EndTime})
	if err != nil {
		return fmt.Errorf("failed to update last fetched time: %v", err)
	}
	log.Println("Last updated time saved successfully.")

	return nil
}

func main() {
	log.Println("Starting cron job...")
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file. Make sure the .env file exists and contains the required variables.")
	}

	var container = Container{
		GET_WEIGHT_GRAPH_API: os.Getenv("GET_WEIGHT_GRAPH_API"),
		MONGO_DB:             os.Getenv("MONGO_DB"),
		MetricsCollection:    os.Getenv("MetricsCollection"),
		UpdateLogCollection:  os.Getenv("UpdateLogCollection"),
	}
	MONGO_URI = os.Getenv("MONGO_URI")

	done := make(chan os.Signal, 15)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	c := cron.New()

	_, err := c.AddFunc("*/1 * * * *", container.CalculateMetricAndPushToDB)
	if err != nil {
		log.Fatal("Error scheduling cron job:", err)
	}

	c.Start()

	fmt.Println("Cron job started. Running every 15 minutes...")

	<-done

	log.Println("Received interruption signal")
	log.Println("Stopping cron job...")
	c.Stop()
	log.Println("Cron job stopped")
	log.Println("Exiting...")
}
