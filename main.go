package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/sqs"
	"github.com/goamz/goamz/exp/sns"
	"github.com/golang/time/rate"
)

var R = flag.Int("r", 0, "rate to produce messages (per second)")
var B = flag.Int("b", 1, "number of messages to include in a batch")

var NUM_SUBSCRIBER_THREADS = 1

func usage() {
	fmt.Printf("Usage:\n\n")
	fmt.Printf("  publish messages at rate R for which it also subscribes:\n")
	fmt.Printf("    -r <R> -b <batch size if any> publish\n\n")
}

const letterBytes = "abcdef0123456789"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func randHexString(n int) string {
	b := make([]byte, n)
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return string(b)
}

// measure latencies of at least our own messages
type latencyMeter struct {
	sent map[string]time.Time
	received map[string]time.Time
	mu sync.Mutex
}

func (lm *latencyMeter) markSent(msg string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.sent[msg] = time.Now()
}

func (lm *latencyMeter) markReceived(msg string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if _, ok := lm.sent[msg]; ok {
		lm.received[msg] = time.Now()
	}
}

func (lm *latencyMeter) stats() (min, mean, max int64) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	for msg, received := range lm.received {
		diff := received.Sub(lm.sent[msg])
		if min == 0 || diff.Nanoseconds() < min {
			min = diff.Nanoseconds()
		}
		if max == 0 || diff.Nanoseconds() > max {
			max = diff.Nanoseconds()
		}
		mean += diff.Nanoseconds() / int64(time.Millisecond)
	}
	min /= int64(time.Millisecond)
	max /= int64(time.Millisecond)
	mean /= int64(len(lm.received))
	return
}

func main() {
	flag.Parse()
	if flag.NArg() != 1 {
		usage()
		return
	}

	topic := os.Getenv("SNS_TOPIC_ARN")
	if len(topic) == 0 {
		fmt.Println("SNS_TOPIC_ARN must be set!")
		return
	}

	region := os.Getenv("AWS_REGION")
	if len(region) == 0 {
		fmt.Println("AWS_REGION must be set!")
		return
	}

	rand.Seed(time.Now().UnixNano())
	
	switch flag.Arg(0) {
	case "publish":
		if *R == 0 {
			usage()
			return
		}
		c := make(chan os.Signal, 1)
		done := make(chan struct{}, 1)
		signal.Notify(c, os.Interrupt)
		go func() {
			<-c
			close(done)
		}()
		latencyMeter := &latencyMeter{
			sent: make(map[string]time.Time),
			received: make(map[string]time.Time),
		}
		var totalReceived, totalSent int
		var startSubscribe, startPublish time.Time
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			totalSent, startPublish = publish(topic, region, *R, *B, latencyMeter, done)
			wg.Done()
		}()
		go func() {
			totalReceived, startSubscribe = subscribe(topic, region, latencyMeter, done)
			wg.Done()
		}()
		wg.Wait()
		receiveRate := float64(totalReceived) / time.Now().Sub(startSubscribe).Seconds()
		sendRate := float64(totalSent) / time.Now().Sub(startPublish).Seconds()
		min, mean, max := latencyMeter.stats()
		fmt.Printf("total msgs sent: %d, achieved rate: %.2f msgs/sec\n", totalSent, sendRate)
		fmt.Printf("total msgs received: %d, achieved rate: %.2f msgs/sec\n", totalReceived, receiveRate)
		fmt.Printf("latency: min %d ms, mean %d ms, max %d ms\n", min, mean, max)
	default:
		usage()
		return
	}
}

// publish
func publish(topic, region string, r, b int, lm *latencyMeter, done chan struct{}) (total int, start time.Time) {
        reg, ok := aws.Regions[region]
	if !ok {
		panic("unknown region")
	}
	auth, err := aws.GetAuth("", "", "", time.Time{})
	if err != nil {
		panic(err)
	}
	mySns := sns.New(auth, reg)
	limiter := rate.NewLimiter(rate.Limit(float64(r)), 100)
	start = time.Now()
	stop := false
	for !stop {
		r := limiter.ReserveN(time.Now(), 1)
		if r.OK() {
			opts := &sns.PublishOpt{}
			opts.Message = randHexString(64)
			opts.TopicArn = topic
			if _, err := mySns.Publish(opts); err != nil {
				panic(err)
			}
			total++
		}
		select {
		case <-done:
			stop = true
		case <-time.After(r.Delay()):
		}
	}
	return
}

// subscribe
func subscribe(topic, region string, lm *latencyMeter, done chan struct{}) (total int, start time.Time) {
        r, ok := aws.Regions[region]
	if !ok {
		panic("unknown region")
	}
	auth, err := aws.GetAuth("", "", "", time.Time{})
	if err != nil {
		panic(err)
	}

	mySqs := sqs.New(auth, r)
	queueName := "queue-" + randHexString(32)
	queue, err := mySqs.CreateQueue(queueName)
	if err != nil {
		panic(err)
	}

	// subscribe
	mySns := sns.New(auth, r)
	if _, err := mySns.Subscribe(queue.Url, "sqs", topic); err != nil {
		panic(err)
	}
	
	var mu sync.Mutex
	for i := 0; i < NUM_SUBSCRIBER_THREADS; i++ {
		go func() {
			for {
				resp, err := queue.ReceiveMessage(1000)
				if err != nil {
					fmt.Println(err)
					return
				}
				if total == 0 {
					mu.Lock()
					t := total
					mu.Unlock()
					if t == 0 {
						start = time.Now()
					}
				}
				for _, msg := range resp.Messages {
					lm.markReceived(msg.Body)
					mu.Lock()
					total++
					mu.Unlock()
				}
			}
		}()
	}
	// leaking goroutines but we're exiting anyway...
        <-done
	if _, err := queue.Delete(); err != nil {
		panic(err)
	}

	mu.Lock()
	t := total
	mu.Unlock()
	
	return t, start
}
