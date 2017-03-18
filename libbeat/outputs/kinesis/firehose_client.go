package kinesis

import (
	"github.com/elastic/beats/libbeat/outputs"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"

)

type FireHoseClient struct {
	Client
	stream  string
	session *session.Session
	codec   outputs.Codec
	service *firehose.Firehose
	config  KinesisConfig
}

func NewFireHoseClient(session *session.Session, config KinesisConfig, writer outputs.Codec) (*FireHoseClient, error) {
	c := FireHoseClient{
		stream: config.Stream,
		session: session,
		codec:  writer,
		config: config,
	}
	return &c, nil
}

func (c *FireHoseClient) Connect() error {

	svc := firehose.New(c.session)

	debugf("Connected to service: %v", svc)

	c.service = svc

	return nil

}

func (c *FireHoseClient) PutMessage(data outputs.Data) error {

	serializedEvent, err := c.codec.Encode(data.Event)
	if err != nil {
		return err
	}

	params := &firehose.PutRecordInput{
		DeliveryStreamName: aws.String(c.stream),
		Record: &firehose.Record{
			Data: []byte(serializedEvent),
		},
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
