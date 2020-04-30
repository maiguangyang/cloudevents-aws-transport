package encoding

type SNSMessage struct {
	Type    string // In case of SNS, the type is "Notification"
	Message string // JSON encoded message
}

func (m *SNSMessage) IsNotification() bool {
	return m.Type == "Notification"
}
