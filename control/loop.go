package control

import "sync"

type ControlMessage struct {
	Message  string
	TS       int64
	DB       int
	Reply    *chan string
	Finished *sync.WaitGroup
}

const DONE = "done"

func (cm ControlMessage) Done(s string) {

	if cm.Finished != nil {
		cm.Finished.Done()
	}
	if cm.Reply != nil {
		*cm.Reply <- s
	}
}
