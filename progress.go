package turboseeder

import (
	"fmt"
	"io"
	"os"
	"runtime/metrics"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Progress is a snapshot passed to OnProgress callbacks.
type Progress struct {
	Table   string
	Done    int64
	Total   int64
	Elapsed time.Duration
	Final   bool // true on the last callback of the run
}

// Fraction is Done/Total in [0,1].
func (p Progress) Fraction() float64 {
	if p.Total == 0 {
		return 0
	}
	return min(1, max(0, float64(p.Done)/float64(p.Total)))
}

// RowsPerSecond is the average throughput so far.
func (p Progress) RowsPerSecond() float64 {
	if p.Elapsed <= 0 {
		return 0
	}
	return float64(p.Done) / p.Elapsed.Seconds()
}

// ETA estimates the remaining time at the current average rate.
func (p Progress) ETA() time.Duration {
	rate := p.RowsPerSecond()
	if rate == 0 || p.Done >= p.Total {
		return 0
	}
	return time.Duration(float64(p.Total-p.Done) / rate * float64(time.Second))
}

// ProgressBar returns an OnProgress callback that draws a single-line bar on
// w (os.Stderr when nil):
//
//	users [██████████░░░░░░░░░░]  52%  520,000/1,000,000  412,301 rows/s  ETA 1s
func ProgressBar(w io.Writer) func(Progress) {
	if w == nil {
		w = os.Stderr
	}
	const width = 24
	return func(p Progress) {
		filled := int(p.Fraction() * width)
		bar := strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
		eta := ""
		if !p.Final {
			eta = "  ETA " + p.ETA().Round(time.Second).String()
		}
		fmt.Fprintf(w, "\r\033[K%s [%s] %3.0f%%  %s/%s  %s rows/s%s",
			p.Table, bar, p.Fraction()*100, commas(p.Done), commas(p.Total),
			commas(int64(p.RowsPerSecond())), eta)
		if p.Final {
			fmt.Fprintf(w, "  %s\n", p.Elapsed.Round(time.Millisecond))
		}
	}
}

func commas(n int64) string {
	s := fmt.Sprint(n)
	if n < 0 {
		return "-" + commas(-n)
	}
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

// tracker counts written rows, samples heap usage and drives callbacks.
type tracker struct {
	table string
	total int64
	fn    func(Progress)
	start time.Time

	done atomic.Int64
	peak atomic.Uint64

	stop chan struct{}
	wg   sync.WaitGroup
}

const heapMetric = "/memory/classes/heap/objects:bytes"

func newTracker(table string, total int64, fn func(Progress)) *tracker {
	t := &tracker{table: table, total: total, fn: fn, start: time.Now(), stop: make(chan struct{})}
	t.sampleHeap()
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-t.stop:
				return
			case <-tick.C:
				t.sampleHeap()
				t.report(false)
			}
		}
	}()
	return t
}

func (t *tracker) add(n int64) { t.done.Add(n) }

func (t *tracker) sampleHeap() {
	s := []metrics.Sample{{Name: heapMetric}}
	metrics.Read(s)
	if s[0].Value.Kind() != metrics.KindUint64 {
		return
	}
	v := s[0].Value.Uint64()
	for {
		old := t.peak.Load()
		if v <= old || t.peak.CompareAndSwap(old, v) {
			return
		}
	}
}

func (t *tracker) report(final bool) {
	if t.fn == nil {
		return
	}
	t.fn(Progress{Table: t.table, Done: t.done.Load(), Total: t.total, Elapsed: time.Since(t.start), Final: final})
}

// finish stops the ticker and emits the final callback when ok is true.
func (t *tracker) finish(ok bool) time.Duration {
	close(t.stop)
	t.wg.Wait()
	t.sampleHeap()
	if ok {
		t.report(true)
	}
	return time.Since(t.start)
}
