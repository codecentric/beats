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
	session *session.Session
	codec   outputs.Codec
	service *kinesis.Kinesis
	config  KinesisConfig
}

func NewStreamClient(session *session.Session, config KinesisConfig, writer outputs.Codec) (*StreamClient, error) {
	c := StreamClient{
		stream:  config.Stream,
		session: session,
		codec:   writer,
		config:  config,
	}
	return &c, nil
}

func (c *StreamClient) Connect() error {

	svc := kinesis.New(c.session)

	debugf("Connected to service: %v", svc)

	c.service = svc

	return nil

}

func (c *StreamClient) PutMessage(data outputs.Data) error {

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
