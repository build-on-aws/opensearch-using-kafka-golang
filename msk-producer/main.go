package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/ec2rolecreds"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	sasl_aws "github.com/twmb/franz-go/pkg/sasl/aws"
)

var mskBroker string
var topic string

var client *kgo.Client

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
		fmt.Println("using default value for AWS_REGION, region")
	}

	fmt.Println("MSK_BROKER", mskBroker)
	fmt.Println("MSK_TOPIC", topic)

	tlsDialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}}

	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region), config.WithCredentialsProvider(ec2rolecreds.New()))

	if err != nil {
		log.Fatal("failed to load config", err.Error())
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(mskBroker, ",")...),
		kgo.SASL(sasl_aws.ManagedStreamingIAM(func(ctx context.Context) (sasl_aws.Auth, error) {

			val, err := cfg.Credentials.Retrieve(context.Background())
			if err != nil {
				fmt.Println("failed to retrieve credentials", err.Error())
				return sasl_aws.Auth{}, err
			}

			return sasl_aws.Auth{
				AccessKey:    val.AccessKeyID,
				SecretKey:    val.SecretAccessKey,
				SessionToken: val.SessionToken,
				UserAgent:    "msk-ec2-producer-app",
			}, nil
		})),

		kgo.Dialer(tlsDialer.DialContext),
	}

	client, err = kgo.NewClient(opts...)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {

	fmt.Println("starting producer app")

	var err error

	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	kadmin := kadm.NewClient(client)
	topics, err := kadmin.ListTopics(context.Background(), topic)
	if err != nil {
		log.Fatal("failed to list topics", err)
	}

	if !topics.Has(topic) {
		_, err := kadmin.CreateTopics(context.Background(), 3, 2, nil, topic)
		if err != nil {
			log.Fatal("create topic invocation failed", err)
		}

	} else {
		fmt.Println("topic already exists", topic)
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("producing data to topic")

		payload, err := ioutil.ReadAll(r.Body)
		if err != nil {
			fmt.Println("unable to read body")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Println("payload", string(payload))
		defer r.Body.Close()

		res := client.ProduceSync(context.Background(), &kgo.Record{Topic: topic, Value: payload})

		for _, r := range res {
			if r.Err != nil {
				fmt.Println("produce error:", r.Err)
				http.Error(w, r.Err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Add("kafka-timestamp", r.Record.Timestamp.String())

			fmt.Println("record produced successfully to offset", r.Record.Offset, "in partition", r.Record.Partition, "of topic", r.Record.Topic)
		}
	})

	fmt.Println("http server ready")

	log.Fatal(http.ListenAndServe(":8080", nil))
}
