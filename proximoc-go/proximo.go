package proximoc

import "github.com/uw-labs/proximo/proto"

// Deprecated: Use https://github.com/uw-labs/proximo/proto instead
type Offset = proto.Offset

const (
	// Deprecated: Use https://github.com/uw-labs/proximo/proto instead
	Offset_OFFSET_DEFAULT = proto.Offset_OFFSET_DEFAULT
	// Deprecated: Use https://github.com/uw-labs/proximo/proto instead
	Offset_OFFSET_NEWEST = proto.Offset_OFFSET_NEWEST
	// Deprecated: Use https://github.com/uw-labs/proximo/proto instead
	Offset_OFFSET_OLDEST = proto.Offset_OFFSET_OLDEST
)

// Deprecated: Use https://github.com/uw-labs/proximo/proto instead
type Message = proto.Message

// Deprecated: Use https://github.com/uw-labs/proximo/proto instead
type ConsumerRequest = proto.ConsumerRequest

// Deprecated: Use https://github.com/uw-labs/proximo/proto instead
type StartConsumeRequest = proto.StartConsumeRequest

// Deprecated: Use https://github.com/uw-labs/proximo/proto instead
type Confirmation = proto.Confirmation

// Deprecated: Use https://github.com/uw-labs/proximo/proto instead
type PublisherRequest = proto.PublisherRequest

// Deprecated: Use https://github.com/uw-labs/proximo/proto instead
type StartPublishRequest = proto.StartPublishRequest

// Deprecated: Use https://github.com/uw-labs/proximo/proto instead
type MessageSourceClient = proto.MessageSourceClient

// Deprecated: Use https://github.com/uw-labs/proximo/proto instead
var NewMessageSourceClient = proto.NewMessageSourceClient

// Deprecated: Use https://github.com/uw-labs/proximo/proto instead
type MessageSource_ConsumeClient = proto.MessageSource_ConsumeClient

// Deprecated: Use https://github.com/uw-labs/proximo/proto instead
type MessageSourceServer = proto.MessageSourceServer

// Deprecated: Use https://github.com/uw-labs/proximo/proto instead
var RegisterMessageSourceServer = proto.RegisterMessageSourceServer

// Deprecated: Use https://github.com/uw-labs/proximo/proto instead
type MessageSource_ConsumeServer = proto.MessageSource_ConsumeServer

// Deprecated: Use https://github.com/uw-labs/proximo/proto instead
type MessageSinkClient = proto.MessageSinkClient

// Deprecated: Use https://github.com/uw-labs/proximo/proto instead
var NewMessageSinkClient = proto.NewMessageSinkClient

// Deprecated: Use https://github.com/uw-labs/proximo/proto instead
type MessageSink_PublishClient = proto.MessageSink_PublishClient

// Deprecated: Use https://github.com/uw-labs/proximo/proto instead
type MessageSinkServer = proto.MessageSinkServer

// Deprecated: Use https://github.com/uw-labs/proximo/proto instead
var RegisterMessageSinkServer = proto.RegisterMessageSinkServer
