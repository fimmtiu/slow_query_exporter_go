package main

import (
	"github.com/hpcloud/tail"
)

type FileWatcher struct {
	tailer *tail.Tail
	query  SlowQuery
	sender Sender
}

func NewFileWatcher(conf Config, sender Sender) (FileWatcher, error) {
	tailConf := tail.Config{
		Follow:    true,
		MustExist: true,
		ReOpen:    true,
	}
	tailer, err := tail.TailFile(conf.LogPath, tailConf)
	if err != nil {
		return FileWatcher{}, err
	}

	return FileWatcher{tailer, SlowQuery{}, sender}, nil
}

func (watcher *FileWatcher) Watch() error {
	for line := range watcher.tailer.Lines {
		watcher.handleLine(line.Text)
	}

	return watcher.tailer.Wait()
}

func (watcher *FileWatcher) handleLine(line string) error {
	done, err := watcher.query.ParseLine(line)
	if done && err == nil {
		watcher.sender.SendQuery(watcher.query)
		watcher.query = SlowQuery{}
	}
	return err
}
