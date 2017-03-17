package kinesis

import (
	"github.com/elastic/beats/libbeat/outputs"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type StreamClient struct {
	Client
	stream  string
	codec   outputs.Codec
	service *kinesis.Kinesis
	config  KinesisConfig
}

func NewStreamClient(config KinesisConfig, writer outputs.Codec) (*StreamClient, error) {
	c := StreamClient{
		stream: config.Stream,
		codec:  writer,
		config: config,
	}
	return &c, nil
}

func (c *StreamClient) Connect() error {

	debugf("Connecting to Kinesis in region:", c.config.Region)

	session := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{Region: aws.String(c.config.Region)},
	}))
	svc := kinesis.New(session)

	debugf("Connected to service: %v", svc)

	c.service = svc

	return nil

}

func (c *StreamClient) PutMessage(data outputs.Data) error {
	putMessageCallCount.Add(1)

	serializedEvent, err := c.codec.Encode(data.Event)
	if err != nil {
		debugf("Got the following error: %v while encoding event: %v with codec: %v", err, data.Event, c.codec)
		statWriteErrors.Add(1)
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
		debugf("Received following kinsesis error: %v and response: %v", err, resp)
		debugf("Will abort processing.")
		statWriteErrors.Add(1)
		return err
	}

	debugf("Received following kinsesis response: %v and error: %v", resp, err)
	statWriteBytes.Add(int64(len(serializedEvent)))

	return err
}
