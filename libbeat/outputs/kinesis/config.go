package kinesis

import (
	"github.com/elastic/beats/libbeat/outputs"
)

type kinesisConfig struct {
	Stream            string                    `config:"stream"                validate:"required"`
	Codec           	outputs.CodecConfig       `config:"codec"`
}

//Default Config for non mandatory settings
var (
	defaultKinesisConfig = kinesisConfig{
		Stream: "",
	}
)
