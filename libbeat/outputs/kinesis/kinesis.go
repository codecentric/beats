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

type kinesis struct {
	config kinesisConfig
}

// New instantiates a new kinesis output instance.
func New(_ common.BeatInfo, cfg *common.Config, topologyExpire int) (outputs.Outputer, error) {
	output := &kinesis{}
	err := output.init(cfg)
	if err != nil {
		return nil, err
	}
	return output, nil
}

func (k *kinesis) init(cfg *common.Config) error {
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
func (k *kinesis) Close() error {
	return nil
}

var nl = []byte{'\n'}

func (k *kinesis) PublishEvent(
	s op.Signaler,
	opts outputs.Options,
	data outputs.Data,
) error {
	op.SigCompleted(s)
	return nil
}
