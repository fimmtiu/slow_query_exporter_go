package main

import (
	"log"
)

func main() {
	conf := GetConfig()
	sender, err := NewSender(conf)
	if err != nil {
		log.Fatal(err)
	}

	// Spin off a separate goroutine which waits for slow queries and sends them to Graylog.
	go sender.ListenForQueries()

	watcher, err := NewFileWatcher(conf, sender)
	if err != nil {
		log.Fatal(err)
	}

	// In the main goroutine, tail the logfile and send the resulting slow queries to the sender thread.
	err = watcher.Watch()
	if err != nil {
		log.Fatal(err)
	}
}
