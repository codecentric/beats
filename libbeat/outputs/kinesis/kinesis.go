package kinesis

import (
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/common/op"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
)

var debugf = logp.MakeDebug("kinesis")

func init() {
	outputs.RegisterOutputPlugin("kinesis", New)
}

type Client interface {
	Connect() error
	PutMessage(outputs.Data) error
}

type kinesisOuput struct {
	config kinesisConfig
	client Client
}

// New instantiates a new kinesis output instance.
func New(_ common.BeatInfo, cfg *common.Config, topologyExpire int) (outputs.Outputer, error) {
	output := &kinesisOuput{}
	err := output.init(cfg)
	if err != nil {
		return nil, err
	}

	err = output.connect()
	if err != nil {
		return nil, err
	}
	return output, nil
}

func (k *kinesisOuput) connect() error {

	codec, err := outputs.CreateEncoder(k.config.Codec)
	if err != nil {
		return err
	}

	client, err := NewFireHoseClient(k.config, codec)
	if err != nil {
		return err
	}

	client.Connect()
	k.client = client

	return nil
}

func (k *kinesisOuput) init(cfg *common.Config) error {
	debugf("initialize kinesis output")

	config := defaultKinesisConfig
	if err := cfg.Unpack(&config); err != nil {
		return err
	}

	k.config = config
	debugf("Assigned configuration to kinesis")


	return nil

}

// Implement Outputer
func (k *kinesisOuput) Close() error {
	return nil
}

var nl = []byte{'\n'}

func (k *kinesisOuput) PublishEvent(
	s op.Signaler,
	opts outputs.Options,
	data outputs.Data,
) error {
	err := k.client.PutMessage(data)
	if err != nil {
		return err
	}
	op.SigCompleted(s)
	return nil
}
