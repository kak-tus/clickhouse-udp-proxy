package nanachi

import "github.com/streadway/amqp"

// ConfirmChanNotifier type represents confirmation notifier.
type ConfirmChanNotifier struct {
	C chan *Confirmation
}

// ReturnChanNotifier type represents return notifier.
type ReturnChanNotifier struct {
	C chan *amqp.Return
}

// ErrorChanNotifier type represents error notifier.
type ErrorChanNotifier struct {
	C chan error
}

// NewConfirmChanNotifier method creates new instance of confirmation notifier.
func NewConfirmChanNotifier(bufSize int) *ConfirmChanNotifier {
	return &ConfirmChanNotifier{
		C: make(chan *Confirmation, bufSize),
	}
}

// Notify method sends notification.
func (n *ConfirmChanNotifier) Notify(c *Confirmation) {
	n.C <- c
}

// Close method performs correct closure of the notifier.
func (n *ConfirmChanNotifier) Close() {
	close(n.C)
}

// NewReturnChanNotifier method creates new instance of return notifier.
func NewReturnChanNotifier(bufSize int) *ReturnChanNotifier {
	return &ReturnChanNotifier{
		C: make(chan *amqp.Return, bufSize),
	}
}

// Notify method sends notification.
func (n *ReturnChanNotifier) Notify(c *amqp.Return) {
	n.C <- c
}

// Close method performs correct closure of the notifier.
func (n *ReturnChanNotifier) Close() {
	close(n.C)
}

// NewErrorChanNotifier method creates new instance of error notifier.
func NewErrorChanNotifier(bufSize int) *ErrorChanNotifier {
	return &ErrorChanNotifier{
		C: make(chan error, bufSize),
	}
}

// Notify method sends notification.
func (n *ErrorChanNotifier) Notify(c error) {
	n.C <- c
}

// Close method performs correct closure of the notifier.
func (n *ErrorChanNotifier) Close() {
	close(n.C)
}
