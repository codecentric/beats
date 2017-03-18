package kinesis

import (
	"github.com/elastic/beats/libbeat/monitoring"
	"github.com/elastic/beats/libbeat/outputs"
)


//used by both clients so extracted into extra file
var (
	putMessageCallCount = monitoring.NewInt(outputs.Metrics, "kinesis.putMessage.call.count")
	statWriteBytes  = monitoring.NewInt(outputs.Metrics, "kinesis.write.bytes")
	statWriteErrors = monitoring.NewInt(outputs.Metrics, "kinesis.write.errors")
)
