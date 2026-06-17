package file

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/feedsource"
)

// Compile-time assertion that *File satisfies feedsource.Source.
var _ feedsource.Source = (*File)(nil)

// File is a source backed by a JSON file containing an array of feedsource.FeedSpec. It
// watches the file's directory so editor atomic-rename writes are detected, and
// debounces rapid events before signaling Changes.
type File struct {
	name    string
	path    string
	watcher *fsnotify.Watcher
	out     chan struct{}
	done    chan struct{}
	once    sync.Once
}

const fileDebounce = 150 * time.Millisecond

// NewFile creates a File source watching path. It does not require the file to
// exist yet (a missing file reads as an empty feed list). The file's parent
// directory must exist because the watch is added to it.
func NewFile(name, path string) (*File, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("file source %s: resolve path: %w", name, err)
	}
	abs = filepath.Clean(abs)

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("file source %s: watcher: %w", name, err)
	}
	// Watch the parent dir: atomic saves replace the file via rename, which a
	// watch on the file itself would miss.
	if err := w.Add(filepath.Dir(abs)); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("file source %s: watch dir: %w", name, err)
	}
	f := &File{
		name:    name,
		path:    abs,
		watcher: w,
		out:     make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	go f.loop()
	return f, nil
}

func (f *File) Name() string { return f.name }

func (f *File) Changes() <-chan struct{} { return f.out }

func (f *File) Feeds(context.Context) ([]config.FeedConfig, error) {
	data, err := os.ReadFile(f.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("file source %s: read: %w", f.name, err)
	}
	var specs []feedsource.FeedSpec
	if err := json.Unmarshal(data, &specs); err != nil {
		return nil, fmt.Errorf("file source %s: parse: %w", f.name, err)
	}
	return feedsource.SpecsToConfigs(specs)
}

func (f *File) Close() error {
	f.once.Do(func() { close(f.done) })
	return f.watcher.Close()
}

func (f *File) loop() {
	var timer *time.Timer
	emit := func() {
		select {
		case f.out <- struct{}{}:
		default:
		}
	}
	for {
		select {
		case <-f.done:
			return
		case ev, ok := <-f.watcher.Events:
			if !ok {
				return
			}
			// Only react to events touching our file (dir watch sees siblings).
			// f.path is already absolute and clean; ev.Name from inotify is absolute.
			if filepath.Clean(ev.Name) != f.path {
				continue
			}
			if timer != nil {
				timer.Stop()
			}
			timer = time.AfterFunc(fileDebounce, emit)
		case _, ok := <-f.watcher.Errors:
			if !ok {
				return
			}
		}
	}
}
