package kinesis

import (
	"github.com/elastic/beats/libbeat/outputs"
)

type kinesisConfig struct {
	Stream string              `config:"stream"                validate:"required"`
	Codec  outputs.CodecConfig `config:"codec"`
	Region string `config:region`
}

//Default Config for non mandatory settings
var (
	defaultKinesisConfig = kinesisConfig{
		Stream: "",
		Region: "eu-central-1",
	}
)
