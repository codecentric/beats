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
		stream:  config.Stream,
		session: session,
		codec:   writer,
		config:  config,
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
	putMessageCallCount.Add(1)

	serializedEvent, err := c.codec.Encode(data.Event)
	if err != nil {
		debugf("Got the following error: %v while encoding event: %v with codec: %v", err, data.Event, c.codec)
		statWriteErrors.Add(1)
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
		debugf("Received following kinsesis error: %v and response: %v", err, resp)
		debugf("Will abort processing.")
		statWriteErrors.Add(1)
		return err
	}

	debugf("Received following kinsesis response: %v and error: %v", resp, err)
	statWriteBytes.Add(int64(len(serializedEvent)))

	return err
}
