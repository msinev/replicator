package main
import "RedisReplica/Compressor"

func QOSBuilder(mc []chan Compressor.TheMessage) <-chan Compressor.TheMessage  {
    lmc:=len(mc)

	if(lmc==1) {
		return mc[0]
	} else if (lmc==2) {
	   out:=make(chan Compressor.TheMessage)
       go QOSScheduler2(mc[1], mc[0], out)
	   return out
	} else if (lmc==3) {
		out:=make(chan Compressor.TheMessage)
		go QOSScheduler3( mc[2], mc[1], mc[0], out )
		return out
	} else if (lmc==4) {
		out:=make(chan Compressor.TheMessage)
		go QOSScheduler4( mc[3], mc[2], mc[1], mc[0], out )
		return out
	} else if (lmc>4) {
		out:=make(chan Compressor.TheMessage)
		go QOSScheduler4( QOSBuilder( mc[lmc/2+1:]), QOSBuilder( mc[2:lmc/2+1] ), mc[1], mc[0], out)
		return out
	}
	return nil
}
/*
func (c *Client) CreateQOS() chan Compressor.TheMessage {
    for i,dbnum:=range c.databases {
		var consumeReplica=make(chan RedisKV, 100)
		var messager=make(chan RedisKV, 100)
		go kvConsumerSender(dbnum, consumeReplica, &wg1)
		go scan(dbnum, consumeReplica, nil);

	}

}
*/

func QOSScheduler1(QOSRT <-chan Compressor.TheMessage, OutBound chan<- Compressor.TheMessage) {

	defer close(OutBound)
	defer log.Info("Done QOSScheduler1")
	log.Info("Entering  QOSScheduler1")
	for x:= range QOSRT {
		log.Noticef("I Value %v was received.\n", x)
		OutBound <- x
		log.Noticef("I Value %v was snt.\n", x)
	}
}

func QOSScheduler4(QOSIdle <-chan Compressor.TheMessage, QOSLo <-chan Compressor.TheMessage, QOSHi <-chan Compressor.TheMessage, QOSRT <-chan Compressor.TheMessage, OutBound chan<- Compressor.TheMessage) {
	defer log.Info("Terminating QOSScheduler4")
	log.Info("Initiating  QOSScheduler4")
    var x Compressor.TheMessage
	var ok bool
	for {
		select {
		case x, ok = <-QOSRT:
			if  !ok {
				QOSScheduler3(QOSIdle, QOSLo, QOSHi,OutBound)
				return
			}
		default:
			log.Notice("A No value ready, moving on level 2.")
			select {
			case x, ok = <-QOSRT:
				if  !ok {
					QOSScheduler3(QOSIdle, QOSLo, QOSHi,OutBound)
					return
				}
			case x, ok = <-QOSHi:
				if  !ok {
					QOSScheduler3(QOSIdle, QOSLo, QOSRT,OutBound)
					return
				}
			default:
				log.Notice("B No value ready, moving on level 3.")
				select {
				case x, ok = <-QOSRT:
					if  !ok {
						QOSScheduler3(QOSIdle, QOSLo, QOSHi,OutBound)
						return
					}
				case x, ok = <-QOSHi:
					if  !ok {
						QOSScheduler3(QOSIdle, QOSLo, QOSRT,OutBound)
						return
					}
				case x, ok = <-QOSLo:
					if  !ok {
						QOSScheduler3(QOSIdle, QOSHi, QOSRT,OutBound)
						return
					}
				default:
					log.Notice("C No value ready, moving on level level 4.")
					select {
					case x, ok = <-QOSRT:
						if  !ok {
							QOSScheduler3(QOSIdle, QOSHi, QOSLo,OutBound)
							return
						}
					case x, ok = <-QOSHi:
						if  !ok {
							QOSScheduler3(QOSIdle, QOSLo, QOSRT,OutBound)
							return

						}
					case x, ok = <-QOSLo:
						if  !ok {
							QOSScheduler3(QOSIdle, QOSHi, QOSRT,OutBound)
							return
						}
					case x, ok = <-QOSIdle:
						if  !ok {
							QOSScheduler3(QOSLo, QOSHi, QOSRT,OutBound)
							return
						}
					}                                }
			}

		}
		log.Noticef("S4 Value %v was received.\n", x)
		OutBound <- x
		log.Noticef("S4 Value %v was snt.\n", x)

	}

}


func QOSScheduler3(QOSLo <-chan Compressor.TheMessage, QOSHi <-chan Compressor.TheMessage, QOSRT <-chan Compressor.TheMessage, OutBound chan<- Compressor.TheMessage) {
	defer log.Info("Terminating QOSScheduler3")
	log.Info("Initiating  QOSScheduler3")
	for {
		var x Compressor.TheMessage
		var ok bool
		select {
		case x, ok = <-QOSRT:
			if  !ok {
				QOSScheduler2(QOSLo, QOSHi,OutBound)
				return
			}
		default:
			log.Notice("A No value ready, moving on level 2.")
			select {
			case x, ok = <-QOSRT:
				if  !ok {
					QOSScheduler2(QOSLo, QOSHi,OutBound)
					return
				}
			case x, ok = <-QOSHi:
				if  !ok {
					QOSScheduler2(QOSLo, QOSRT,OutBound)
					return
				}
			default:
				log.Notice("B No value ready, moving on level 3.")
				select {
				case x, ok = <-QOSRT:
					if  !ok {
						QOSScheduler2(QOSLo, QOSHi,OutBound)
						return
					}
				case x, ok = <-QOSHi:
					if  !ok {
						QOSScheduler2(QOSRT, QOSLo,OutBound)
						return
					}
				case x, ok = <-QOSLo:
					if  !ok {
						QOSScheduler2(QOSRT, QOSHi,OutBound)
						return
					}
                }
			}

		}

		log.Noticef("S3 Value %v was received.\n", x)
		OutBound <- x
		log.Noticef("S3 Value %v was snt.\n", x)
	}

}

func QOSScheduler2(QOSHi <-chan Compressor.TheMessage, QOSRT <-chan Compressor.TheMessage, OutBound chan<- Compressor.TheMessage) {

	var x Compressor.TheMessage
	var ok bool

	log.Info("Initiating  QOSScheduler2")
	defer log.Info("Terminating QOSScheduler2")

	for {
		select {
		case x, ok = <-QOSRT:
			if  !ok {
				QOSScheduler1(QOSHi,OutBound)
				return
			}
		default:
			log.Notice("A No value ready, moving on level 2.")
			select {
			case x, ok = <-QOSRT:
				if  !ok {
					QOSScheduler1(QOSHi,OutBound)
					return
				}
			case x, ok = <-QOSHi:
				if  !ok {
					QOSScheduler1(QOSRT,OutBound)
					return
				}

			}

		}
		log.Noticef("S2 Value %v was received.\n", x)
		OutBound <- x
		log.Noticef("S2 Value %v was snt.\n", x)
	}

}

