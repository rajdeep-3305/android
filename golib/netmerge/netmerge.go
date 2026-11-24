package netmerge

import (
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"sync"
	"time"

	"netmerge-android/internal/engine"
	"netmerge-android/internal/nics"
	"netmerge-android/internal/tun"
)

// Modes matching Kotlin side:
// 0: Turbo Boost (Max Speed Proportional Bonding)
// 1: Smart Adaptive (QoS / Latency Steering + Bulk Proportional)
// 2: Data Saver (Wi-Fi Preferred, Failover to Mobile)
// 3: Split Evenly (50/50 Round Robin)
const (
	ModeTurboBoost    = 0
	ModeSmartAdaptive = 1
	ModeDataSaver     = 2
	ModeSplitEvenly   = 3
)

var (
	mu          sync.Mutex
	sched       = engine.NewScheduler(nil)
	bal         = engine.NewBalancer(sched)
	dev         tun.Tun
	stop        chan struct{}
	indexes     = map[string]int{} // network name -> stable index
	last        []nics.Nic         // last list given to the scheduler, to re-apply weights
	lastJS      string             // last SetNetworks input, re-applied when the mode changes
	currentMode = ModeTurboBoost
)

func init() { sched.SetMode(engine.ModeTurboBoost) }

// network is what Kotlin reports for each usable network.
type network struct {
	Name     string  `json:"name"`
	Handle   string  `json:"handle"` // Network#getNetworkHandle, as a decimal string
	V4       bool    `json:"v4"`
	V6       bool    `json:"v6"`
	Weight   float64 `json:"weight"`
	DownKbps float64 `json:"downKbps"`
}

func SetNetworks(js string) error {
	var in []network
	if err := json.Unmarshal([]byte(js), &in); err != nil {
		return err
	}
	mu.Lock()
	defer mu.Unlock()
	var maxDown float64
	for _, n := range in {
		maxDown = max(maxDown, n.DownKbps)
	}
	list := make([]nics.Nic, 0, len(in))
	for _, n := range in {
		h, err := strconv.ParseUint(n.Handle, 10, 64)
		if err != nil {
			return errors.New("bad network handle " + n.Handle)
		}
		idx, ok := indexes[n.Name]
		if !ok {
			idx = len(indexes) + 1
			indexes[n.Name] = idx
		}
		nics.SetHandle(idx, h)
		w := n.Weight
		if w <= 0 {
			w = 1.0
			if (currentMode == ModeTurboBoost || currentMode == ModeSmartAdaptive) && maxDown > 0 && n.DownKbps > 0 {
				w = max(0.1, n.DownKbps/maxDown)
			}
		}
		nic := nics.Nic{Name: n.Name, Index: idx, Weight: w}
		if n.V4 {
			nic.IP = net.IPv4zero
		}
		if n.V6 {
			nic.IP6 = net.IPv6unspecified
		}
		list = append(list, nic)
	}
	sched.Update(list)
	last, lastJS = list, js
	sched.CloseGone()
	return nil
}

func SetMode(mode int) {
	mu.Lock()
	currentMode = mode
	sched.SetMode(engine.Mode(mode))
	js, list := lastJS, last
	mu.Unlock()
	if js != "" {
		SetNetworks(js)
	} else {
		sched.Update(list)
	}
}

func GetMode() int {
	mu.Lock()
	defer mu.Unlock()
	return currentMode
}

func SetAutoWeights(on bool) {
	if on {
		SetMode(ModeTurboBoost)
	} else {
		SetMode(ModeSplitEvenly)
	}
}

func Start(fd int, mtu int) error {
	mu.Lock()
	defer mu.Unlock()
	if stop != nil {
		close(stop)
		stop = nil
	}
	if err := dev.Start(bal, fd, mtu); err != nil {
		return err
	}
	stop = make(chan struct{})
	go sched.RunProbes(stop)
	go tick(stop)
	return nil
}

func tick(stop <-chan struct{}) {
	t := time.NewTicker(250 * time.Millisecond) // 4Hz silky smooth dynamic tracking
	defer t.Stop()
	last := time.Now()
	for {
		select {
		case <-stop:
			return
		case now := <-t.C:
			sched.Tick(now.Sub(last))
			last = now
		}
	}
}

func Stop() {
	mu.Lock()
	defer mu.Unlock()
	if stop != nil {
		close(stop)
		stop = nil
	}
	dev.Stop()
}

func IsRunning() bool {
	mu.Lock()
	defer mu.Unlock()
	return stop != nil
}

func SetIPv6Mode(mode string) error {
	m, ok := engine.ParseV6Mode(mode)
	if !ok {
		return errors.New("IPv6 mode must be auto, on or off")
	}
	bal.SetV6Mode(m)
	return nil
}

type stat struct {
	Name     string  `json:"name"`
	State    string  `json:"state"`
	State6   string  `json:"state6"`
	Active   int64   `json:"active"`
	Total    int64   `json:"total"`
	Failed   int64   `json:"failed"`
	BytesIn  int64   `json:"bytesIn"`
	BytesOut int64   `json:"bytesOut"`
	RateIn   float64 `json:"rateIn"`
	RateOut  float64 `json:"rateOut"`
	Weight   float64 `json:"weight"`
	Present  bool    `json:"present"`
	RTT      int64   `json:"rtt"`
	Ratio    float64 `json:"ratio"`
}

func Stats() string {
	snap := sched.Snapshot()
	var totalThroughput float64
	for _, s := range snap {
		if s.Present {
			totalThroughput += s.RateIn + s.RateOut
		}
	}
	out := make([]stat, 0, len(snap))
	for _, s := range snap {
		ratio := 0.0
		if totalThroughput > 0 && s.Present {
			ratio = ((s.RateIn + s.RateOut) / totalThroughput) * 100.0
		}
		rtt := s.RTT
		if rtt <= 0 && s.RTT6 > 0 {
			rtt = s.RTT6
		}
		out = append(out, stat{
			Name: s.Name, State: s.State, State6: s.State6,
			Active: s.Active, Total: s.Total, Failed: s.Failed,
			BytesIn: s.BytesIn, BytesOut: s.BytesOut, RateIn: s.RateIn, RateOut: s.RateOut,
			Weight: s.Weight, Present: s.Present,
			RTT: rtt, Ratio: ratio,
		})
	}
	b, _ := json.Marshal(out)
	return string(b)
}
