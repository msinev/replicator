package blockaggregator

import "time"

func Blocker(in <-chan string, out chan<- []string) {
	defer close(out)
	var block []string
	for {
		select {
		case i, ok := <-in:
			{
				if !ok {
					return
				}
				block = []string{i}
			}
		}
		for {
			select {
			case i, ok := <-in:
				{
					if !ok {
						out <- block
						return
					}
					block = append(block, i)
				}
			case out <- block:
				{
					block = []string{}
					break
				}
			}
		}
	}
}

func UniqueBlocker(in <-chan string, out chan<- []string) {
	defer close(out)
	uniq := make(map[string]bool)
	var block []string
	for {
		select {
		case i, ok := <-in:
			{
				if !ok {
					return
				}
				block = []string{i}
				uniq[i] = true
			}
		}
		for {
			select {
			case i, ok := <-in:
				{
					if !ok {
						out <- block
						return
					}
					if !uniq[i] {
						uniq[i] = true
						block = append(block, i)
					}

				}
			case out <- block:
				{
					block = []string{}
					uniq = make(map[string]bool)
					break
				}
			}
		}
	}
}

func UniqueTBlocker(in <-chan string, out chan<- map[string]int64) {
	defer close(out)
	uniq := make(map[string]int64)

	for {
		select {
		case i, ok := <-in:
			{
				if !ok {
					return
				}

				uniq[i] = time.Now().UnixNano()
			}
		}
		for {
			select {
			case i, ok := <-in:
				{
					if !ok {
						out <- uniq
						return
					}
					uniq[i] = time.Now().UnixNano()
				}
			case out <- uniq:
				{
					uniq = make(map[string]int64)
					break
				}
			}
		}
	}
}
