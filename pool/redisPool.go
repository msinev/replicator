package pool

import (
	"github.com/garyburd/redigo/redis"
	"log"
	"os"
	"time"
)

func Map(vs []string, f func(string) string) []string {
	vsm := make([]string, len(vs))
	for i, v := range vs {
		vsm[i] = f(v)
	}
	return vsm
}

// Factory is a function to create new connections.
type RedisOperation func(redis.Conn) error

var ExecutorGo = make(chan RedisOperation)

func GetEnvDefault(env string, def string) string {
	val := os.Getenv(env)
	if val != "" {
		println("Returning actual \"" + val + "\" for " + env)
		return val
	}
	println("Returning default \"" + def + "\" for " + env)
	return def
}

//rate :=

var redisFuseThrottle = time.Tick(time.Second * 2)

func RedisExecutor(URL string, runEexcutor chan RedisOperation) {
	defer panic("Executor crashed") // TODO: Remove before flight! Only for DEBUG !!!
	<-redisFuseThrottle
	c, err := redis.Dial("tcp", URL)

	if err != nil {
		log.Fatal(err)
		return
	}

	log.Println("Connected to REDIS")

	defer c.Close()

	for {
		foo, ok := <-runEexcutor
		if !ok {
			log.Println("Redis executor channel closed. Terminating executor")
			return
		}

		err = foo(c)
		if err != nil {
			log.Fatal(err)
			go RedisExecutor(URL, runEexcutor)
			log.Println(err)
			return
		}
	}
}
