package proximoc

import "github.com/uw-labs/proximo/proto"

type Offset = proto.Offset

const (
	Offset_OFFSET_DEFAULT = proto.Offset_OFFSET_DEFAULT
	Offset_OFFSET_NEWEST  = proto.Offset_OFFSET_NEWEST
	Offset_OFFSET_OLDEST  = proto.Offset_OFFSET_OLDEST
)

type Message = proto.Message

type ConsumerRequest = proto.ConsumerRequest

type StartConsumeRequest = proto.StartConsumeRequest

type Confirmation = proto.Confirmation

type PublisherRequest = proto.PublisherRequest

type StartPublishRequest = proto.StartPublishRequest

type MessageSourceClient = proto.MessageSourceClient

var NewMessageSourceClient = proto.NewMessageSourceClient

type MessageSource_ConsumeClient = proto.MessageSource_ConsumeClient

type MessageSourceServer = proto.MessageSourceServer

var RegisterMessageSourceServer = proto.RegisterMessageSourceServer

type MessageSource_ConsumeServer = proto.MessageSource_ConsumeServer

type MessageSinkClient = proto.MessageSinkClient

var NewMessageSinkClient = proto.NewMessageSinkClient

type MessageSink_PublishClient = proto.MessageSink_PublishClient

type MessageSinkServer = proto.MessageSinkServer

var RegisterMessageSinkServer = proto.RegisterMessageSinkServer
