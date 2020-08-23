package reader

import "strings"

type splitType = string

func StringSplitter(Subscriber <-chan chan<- splitType, Source <-chan splitType, Abort chan<- int) {
	subscribers := make(map[int]chan<- splitType)

	defer close(Abort)
	count := 0
	loopDone := false

	for !loopDone {
		select {
		case newMsg, ok := <-Source:
			{
				if !ok {
					loopDone = true
					break
				}

				var closemeset []int
				for k, v := range subscribers {
					select {
					case v <- newMsg:
						log.Infof("Sending messages to %d subscriber", k)
					default:
						closemeset = append(closemeset, k)
					}
				}

				for _, v := range closemeset {
					close(subscribers[v])
					delete(subscribers, v)
					log.Infof("Closing %d subscriber", v)
				}
				closemeset = nil

			}
		case newConsumer, ok := <-Subscriber:
			if !ok {
				loopDone = true
				break
			}
			count++
			log.Infof("Appending new %d consumer", count)
			subscribers[count] = newConsumer
		}

	}
	log.Warningf("Closing %d subscribers from splittter", len(subscribers))
	for _, v := range subscribers {
		close(v)
	}
}

func StringMerger(Subscriber chan<- []splitType, Source <-chan splitType, stringPrefix string) {
	TypeHash := map[splitType]bool{}
	Grouped := []splitType{}
	defer close(Subscriber)
	for {
		if len(Grouped) > 0 {
			select {
			case Subscriber <- Grouped:
				Grouped = []splitType{}
				TypeHash = map[splitType]bool{}
			case S, ok := <-Source:
				if !ok {
					Subscriber <- Grouped
					return
				}
				if strings.HasPrefix(S, stringPrefix) && !TypeHash[S] {
					TypeHash[S] = true
					Grouped = append(Grouped, S)
				}
			}
		} else {
			select {
			case S, ok := <-Source:
				if !ok {
					return
				}
				if strings.HasPrefix(S, stringPrefix) && !TypeHash[S] {
					TypeHash[S] = true
					Grouped = append(Grouped, S)
				}
			}
		}
	}
}
