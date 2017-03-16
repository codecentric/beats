package kinesis

import (
	"github.com/elastic/beats/libbeat/outputs"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type client struct {
	stream  string
	codec   outputs.Codec
	config kinesisConfig
	service *kinesis.Kinesis
}

func newKinesisClient(config kinesisConfig, writer outputs.Codec) (client, error) {
	c := client{
		stream: config.Stream,
		codec:  writer,
		config: config,
	}
	return c, nil
}

func (c *client) connect() error {
	debugf("Connecting to Kinesis in region:", c.config.Region)

	//TODO: Replace with configurable settings
	session := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{Region: aws.String(c.config.Region)},
	}))
	svc := kinesis.New(session)

	debugf("Connected to service: %v", svc)

	c.service = svc

	return nil

}

func (c *client) putMessage(data outputs.Data) error {

	serializedEvent, err := c.codec.Encode(data.Event)
	if err != nil {
		return err
	}

	params := &kinesis.PutRecordInput{
		Data:         []byte(serializedEvent),
		PartitionKey: aws.String("1"),
		StreamName:   aws.String(c.stream),
	}

	debugf("Preparing to invoke the service: %v with the params %v", c.service, params)

	resp, err := c.service.PutRecord(params)
	if err != nil {
		debugf("Error sending records to kinesis: ", err)
	}

	debugf("Received following kinsesis response: %v and error: %v", resp, err)

	if err != nil {
		debugf("Will abort processing due to: %v", err)
		return err
	}

	return nil
}
