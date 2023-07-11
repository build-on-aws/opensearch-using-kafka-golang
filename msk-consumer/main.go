package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/ec2rolecreds"
	"github.com/opensearch-project/opensearch-go/v2/opensearchapi"
	"github.com/twmb/franz-go/pkg/kgo"

	opensearch "github.com/opensearch-project/opensearch-go/v2"
	requestsigner "github.com/opensearch-project/opensearch-go/v2/signer/awsv2"
	sasl_aws "github.com/twmb/franz-go/pkg/sasl/aws"
)

const consumerGroupName = "msk-ec2-app-consumer-group"

var region string
var mskBroker string
var topic string

var client *kgo.Client
var creds aws.Credentials

var indexName string
var endpoint string

func init() {

	mskBroker = os.Getenv("MSK_BROKER")
	if mskBroker == "" {
		log.Fatal("missing env var MSK_BROKER")
	}

	topic = os.Getenv("MSK_TOPIC")
	if mskBroker == "" {
		log.Fatal("missing env var MSK_TOPIC")
	}

	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
		fmt.Println("using default value for AWS_REGION", region)
	}

	indexName = os.Getenv("OPENSEARCH_INDEX_NAME")
	if indexName == "" {
		log.Fatal("missing env var OPENSEARCH_INDEX_NAME")
	}

	endpoint = os.Getenv("OPENSEARCH_ENDPOINT_URL")
	if endpoint == "" {
		log.Fatal("missing env var OPENSEARCH_ENDPOINT_URL")
	}

	fmt.Println("MSK_BROKER", mskBroker)
	fmt.Println("MSK_TOPIC", topic)
	fmt.Println("OPENSEARCH_INDEX_NAME", indexName)
	fmt.Println("OPENSEARCH_ENDPOINT_URL", endpoint)

	var err error

	cfg, err = config.LoadDefaultConfig(context.Background(), config.WithRegion(region), config.WithCredentialsProvider(ec2rolecreds.New()))

	if err != nil {
		log.Fatal(err)
	}

	creds, err = cfg.Credentials.Retrieve(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("using credentials from:", creds.Source)

	initializeKafkaClient()
	initializeOpenSearchClient()
}

var cfg aws.Config

func initializeKafkaClient() {

	tlsDialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}}

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(mskBroker, ",")...),
		kgo.SASL(sasl_aws.ManagedStreamingIAM(func(ctx context.Context) (sasl_aws.Auth, error) {

			return sasl_aws.Auth{
				AccessKey:    creds.AccessKeyID,
				SecretKey:    creds.SecretAccessKey,
				SessionToken: creds.SessionToken,
				UserAgent:    "msk-ec2-consumer-app",
			}, nil
		})),

		kgo.Dialer(tlsDialer.DialContext),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(consumerGroupName),
		kgo.OnPartitionsAssigned(partitionsAssigned),
		kgo.OnPartitionsRevoked(partitionsRevoked),
		kgo.OnPartitionsLost(partitionsLost),
	}

	var err error

	client, err = kgo.NewClient(opts...)
	if err != nil {
		log.Fatal(err)
	}
}

var openSearchClient *opensearch.Client

const opensearchServerlessServiceName = "aoss"

func initializeOpenSearchClient() {

	signer, err := requestsigner.NewSignerWithService(cfg, opensearchServerlessServiceName)
	if err != nil {
		log.Fatal(err)
	}

	openSearchClient, err = opensearch.NewClient(opensearch.Config{
		Addresses: []string{endpoint},
		Signer:    signer,
	})
	if err != nil {
		log.Fatal(err)
	}
}

func indexData(data []byte) {

	bodyReader := bytes.NewReader(data)

	indexRequest := opensearchapi.IndexRequest{
		Index: indexName,
		Body:  bodyReader,
	}

	_, err := indexRequest.Do(context.Background(), openSearchClient)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("movie data indexed")
}

func main() {

	go func() {
		fmt.Println("kafka consumer goroutine started. waiting for records")
		for {

			err := client.Ping(context.Background())
			if err != nil {
				log.Fatal(err)
			}

			consumeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			fetches := client.PollRecords(consumeCtx, 0)

			if fetches.IsClientClosed() {
				fmt.Println("kafka client closed")
				return
			}
			fetches.EachError(func(t string, p int32, err error) {
				fmt.Printf("fetch err topic %s partition %d: %v\n", t, p, err)
			})

			fetches.EachRecord(func(r *kgo.Record) {
				fmt.Printf("got record from partition %v key=%s val=%s\n", r.Partition, string(r.Key), string(r.Value))

				indexData(r.Value)

				fmt.Println("committing offsets")

				err = client.CommitUncommittedOffsets(context.Background())
				if err != nil {
					fmt.Printf("commit records failed: %v\n", err)
				}

			})

		}
	}()

	end := make(chan os.Signal, 1)
	signal.Notify(end, syscall.SIGINT, syscall.SIGTERM)

	<-end
	fmt.Println("closing client")

	client.Close()
	fmt.Println("program ended")
}

func partitionsAssigned(ctx context.Context, c *kgo.Client, m map[string][]int32) {
	fmt.Printf("paritions assigned for topic %s %v\n", topic, m[topic])
}

func partitionsRevoked(ctx context.Context, c *kgo.Client, m map[string][]int32) {
	fmt.Printf("paritions revoked for topic %s %v\n", topic, m[topic])
}

func partitionsLost(ctx context.Context, c *kgo.Client, m map[string][]int32) {
	fmt.Printf("paritions lost for topic %s %v\n", topic, m[topic])
}

type Movie struct {
	Directors       []string  `json:"directors,omitempty"`
	ReleaseDate     time.Time `json:"release_date,omitempty"`
	Rating          float64   `json:"rating,omitempty"`
	Genres          []string  `json:"genres,omitempty"`
	ImageURL        string    `json:"image_url,omitempty"`
	Plot            string    `json:"plot,omitempty"`
	Title           string    `json:"title,omitempty"`
	Rank            int       `json:"rank,omitempty"`
	RunningTimeSecs int       `json:"running_time_secs,omitempty"`
	Actors          []string  `json:"actors,omitempty"`
	Year            int       `json:"year,omitempty"`
}
