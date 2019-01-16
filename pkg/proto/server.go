package proto

// MessageServer is a compound interface that represents a server
// that can both publish and consume messages
type MessageServer interface {
	MessageSourceServer
	MessageSinkServer
}
