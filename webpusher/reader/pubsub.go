package reader

import (
	"github.com/op/go-logging"
	"sync"
)

var log = logging.MustGetLogger("READER.WEB")

type PubSubStr struct {
	subscribers map[chan<- string]string
	mutex       sync.Mutex
	state       int
}

func (subs *PubSubStr) InitPubSub(source <-chan string, sname string) {
	subs.subscribers = make(map[chan<- string]string)
	subs.state = -1
	subs.AddSource(source, sname)
}

// Adds source in M to N sources to drains
func (subs *PubSubStr) AddSource(source <-chan string, sname string) {
	subs.mutex.Lock()
	if subs.state == -1 {
		go splitterWorkerStr(subs, source, sname)
		subs.state = 1
	} else if subs.state > 0 {
		go splitterWorkerStr(subs, source, sname)
		subs.state++
		log.Info("Started additional pubsub source %s", sname)
	} else {
		log.Critical("Pubsub invalid state")
	}
	subs.mutex.Unlock()
	log.Info("Started pubsub")
}

func (subs *PubSubStr) AddSubscriber(subscriber chan<- string, name string) {
	subs.mutex.Lock()
	subs.subscribers[subscriber] = name
	subs.mutex.Unlock()
}

//Internal function to run subscription message splitter
func splitterWorkerStr(subs *PubSubStr, source <-chan string, name string) {
	log.Infof("Started pubsub goroutine for %s", name)
	defer log.Infof("Terminated pubsub goroutine for %s", name)

	for s := range source {
		subs.mutex.Lock()
		for ch, name := range subs.subscribers {
			select {
			case ch <- s:
			default:
				log.Infof("Subscriber %s nor ready - closing  and removing\n", name)
				close(ch)
				delete(subs.subscribers, ch)
			}
		}
		subs.mutex.Unlock()
	}

	subs.mutex.Lock()
	log.Infof("Pubsub input source %s closed", name)

	if subs.state == 1 {

		log.Info("Last input closed - closing and deleting all subscribed")
		subs.state = -2
		for k, sname := range subs.subscribers {
			log.Infof("Last subscription %s to %s termination", name, sname)
			close(k)
			delete(subs.subscribers, k)
		}
	} else if subs.state > 1 {
		log.Info("Not last input", subs.state)
		subs.state--
	}
	subs.mutex.Unlock()
}

type PubSubVersion struct {
	subscribers map[chan<- VersionData]string
	mutex       sync.Mutex
	state       int
}

func (subs *PubSubVersion) InitPubSub(source <-chan VersionData, sname string) {
	subs.subscribers = make(map[chan<- VersionData]string)
	subs.state = -1
	//subs.mutex=&sync.Mutex{}
	subs.AddSource(source, sname)
}

// Adds source in M to N sources to drains
func (subs *PubSubVersion) AddSource(source <-chan VersionData, sname string) {
	subs.mutex.Lock()
	if subs.state == -1 {
		go splitterWorkerVersion(subs, source, sname)
		subs.state = 1
	} else if subs.state > 0 {
		go splitterWorkerVersion(subs, source, sname)
		subs.state++
		log.Info("Started additional pubsub version source %s", sname)
	} else {
		log.Critical("Pubsub invalid state version")
	}
	subs.mutex.Unlock()
	log.Info("Started pubsub versio")
}

func (subs *PubSubVersion) AddSubscriber(subscriber chan<- VersionData, name string) {
	subs.mutex.Lock()
	subs.subscribers[subscriber] = name
	subs.mutex.Unlock()
}

//Internal function to run subscription message splitter
func splitterWorkerVersion(subs *PubSubVersion, source <-chan VersionData, name string) {
	log.Infof("Started pubsub version goroutine for %s", name)
	defer log.Infof("Terminated pubsub version goroutine for %s", name)

	for s := range source {
		subs.mutex.Lock()
		for ch, name := range subs.subscribers {
			select {
			case ch <- s:
			default:
				log.Infof("Subscriber pubsub version %s nor ready - closing  and removing\n", name)
				close(ch)
				delete(subs.subscribers, ch)
			}
		}
		subs.mutex.Unlock()
	}

	subs.mutex.Lock()
	log.Infof("Pubsub version input source %s closed", name)

	if subs.state == 1 {

		log.Infof("Last input closed - closing and deleting all version %s subscribed", name)
		subs.state = -2
		for k, sname := range subs.subscribers {
			log.Infof("Last subscription %s to %s termination", name, sname)
			close(k)
			delete(subs.subscribers, k)
		}
	} else if subs.state > 1 {
		log.Infof("Not last input (%d)", subs.state)
		subs.state--
	}
	subs.mutex.Unlock()
}
