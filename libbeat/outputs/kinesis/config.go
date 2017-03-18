package kinesis

import (
	"fmt"

	"github.com/elastic/beats/libbeat/outputs"
)

type KinesisConfig struct {
	Stream string              `config:"stream"                validate:"required"`
	Key    string              `config:"key"                validate:"required"`
	Secret string              `config:"secret"                validate:"required"`
	Codec  outputs.CodecConfig `config:"codec"`
	Region string              `config:region`
	Mode   string              `config:mode`
}

//Default Config for non mandatory settings
var (
	defaultKinesisConfig = KinesisConfig{
		Stream: "",
		Region: "eu-central-1",
		Mode:   "firehose",
	}
)

func (c *KinesisConfig) Validate() error {
	if c.Mode != "firehose" && c.Mode != "stream" {
		return fmt.Errorf("mode '%v' unknown, please use firehose or stream", c.Mode)
	}

	return nil
}
