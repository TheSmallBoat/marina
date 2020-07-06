package marina

/*
type OpCode = uint8

const (
	OpCodeHandshake OpCode = iota
	OpCodePublishServiceRequest
	OpCodePublishServiceResponse
	OpCodeTopic
	OpCodeQos
)*/
type packet struct {
	qos   [1]byte
	topic []byte
	body  []byte
}
