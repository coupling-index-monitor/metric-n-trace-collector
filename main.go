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
	StartTime int64     `bson:"start_time"`
	EndTime   int64     `bson:"end_time"`
	Data      GraphData `bson:"data"`
}

type UpdateLog struct {
	LastFetchTime int64 `bson:"last_fetch_time"`
}

type Container struct {
	GraphAPIURL         string
	DatabaseName        string
	MetricsCollection   string
	UpdateLogCollection string
	MongoClient         *mongo.Client
	HTTPClient          *http.Client
}

func main() {
	log.Println("Starting service...")

	// Load environment
	if err := godotenv.Load(); err != nil {
		log.Fatal("Failed to load .env file")
	}

	requiredEnv := []string{"MONGO_URI", "GET_WEIGHT_GRAPH_API", "MONGO_DB", "MetricsCollection", "UpdateLogCollection"}
	for _, env := range requiredEnv {
		if os.Getenv(env) == "" {
			log.Fatalf("Missing required environment variable: %s", env)
		}
	}

	// Setup MongoDB connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientOpts := options.Client().ApplyURI(os.Getenv("MONGO_URI")).
		SetRetryWrites(true).
		SetConnectTimeout(10 * time.Second)

	mongoClient, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		log.Fatalf("Mongo connection failed: %v", err)
	}

	// Setup container
	container := &Container{
		GraphAPIURL:         os.Getenv("GET_WEIGHT_GRAPH_API"),
		DatabaseName:        os.Getenv("MONGO_DB"),
		MetricsCollection:   os.Getenv("MetricsCollection"),
		UpdateLogCollection: os.Getenv("UpdateLogCollection"),
		MongoClient:         mongoClient,
		HTTPClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}

	// Setup signal handling
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// Start cron
	c := cron.New()
	_, err = c.AddFunc("*/1 * * * *", container.CalculateMetricAndPushToDB)
	if err != nil {
		log.Fatalf("Failed to schedule cron: %v", err)
	}
	c.Start()
	log.Println("Cron job running every 1 minute")

	<-stop
	log.Println("Shutdown signal received")

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c.Stop()
	log.Println("Cron stopped")

	if err := mongoClient.Disconnect(ctx); err != nil {
		log.Printf("Mongo disconnect error: %v", err)
	} else {
		log.Println("Mongo disconnected cleanly")
	}
}

func (c *Container) CalculateMetricAndPushToDB() {
	log.Println("Starting scheduled metric fetch...")

	end := time.Now().UnixMicro()

	start, skip := c.getStartTime(end)
	if skip {
		log.Println("Skipping execution based on start time")
		return
	}

	graphData, err := c.fetchGraphData(start, end)
	if err != nil {
		log.Printf("Error fetching graph data: %v", err)
		return
	}

	if len(graphData.Nodes) == 0 || len(graphData.Edges) == 0 {
		log.Println("Empty graph data — skipping DB insert")
		return
	}

	if err := c.saveMetrics(Metrics{StartTime: start, EndTime: end, Data: graphData}); err != nil {
		log.Printf("Failed to save metrics: %v", err)
	}
}

func (c *Container) getStartTime(end int64) (start int64, skip bool) {
	const maxAllowedGap = 7 * 24 * 60 * 60 * 1_000_000 // 7 days in microseconds

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	col := c.MongoClient.Database(c.DatabaseName).Collection(c.UpdateLogCollection)
	opts := options.FindOne().SetSort(bson.D{{Key: "last_fetch_time", Value: -1}})
	var last UpdateLog
	err := col.FindOne(ctx, bson.M{}, opts).Decode(&last)

	if err == nil {
		log.Printf("Last fetch time: %v", last.LastFetchTime)
		start = last.LastFetchTime
	} else {
		log.Println("No last fetch time found; using default 15 mins")
		start = end - 15*60*1_000_000
	}

	if end-start > maxAllowedGap {
		start = end - maxAllowedGap
		log.Printf("Capped start time to max allowed gap: %v", start)
	}

	return start, false
}

func (c *Container) fetchGraphData(start, end int64) (GraphData, error) {
	url := fmt.Sprintf(c.GraphAPIURL, start, end)
	log.Printf("Fetching graph from %s", url)

	resp, err := c.HTTPClient.Get(url)
	if err != nil {
		return GraphData{}, fmt.Errorf("HTTP error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return GraphData{}, fmt.Errorf("non-200 response: %s, body: %s", resp.Status, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return GraphData{}, fmt.Errorf("failed reading response: %w", err)
	}

	var response WeightedGraphResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return GraphData{}, fmt.Errorf("failed to parse response: %w", err)
	}

	return response.Data, nil
}

func (c *Container) saveMetrics(metrics Metrics) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db := c.MongoClient.Database(c.DatabaseName)

	_, err := db.Collection(c.MetricsCollection).InsertOne(ctx, metrics)
	if err != nil {
		return fmt.Errorf("inserting metrics failed: %w", err)
	}
	log.Println("Metrics inserted")

	_, err = db.Collection(c.UpdateLogCollection).InsertOne(ctx, UpdateLog{LastFetchTime: metrics.EndTime})
	if err != nil {
		return fmt.Errorf("updating fetch time failed: %w", err)
	}
	log.Println("Last fetch time updated")
	return nil
}
