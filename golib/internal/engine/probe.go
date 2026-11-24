package engine

import (
	"net"
	"time"

	"netmerge-android/internal/nics"
)

// Internet reachability probes: a TCP connect to well-known anycast IPs bound to the NIC.
// Probing port 53 (DNS) and 443 (HTTPS) allows rapid SYN-ACK RTT tracking without TLS handshake.
var probeTargets = [2][]string{
	Fam4: {"1.1.1.1:53", "8.8.8.8:53", "1.1.1.1:443", "8.8.8.8:443"},
	Fam6: {"[2606:4700:4700::1111]:53", "[2001:4860:4860::8888]:53", "[2606:4700:4700::1111]:443", "[2001:4860:4860::8888]:443"},
}

const (
	probeTimeout   = 1200 * time.Millisecond // fast circuit breaker detection
	probeEveryUp   = 10 * time.Second
	probeEveryDown = 2 * time.Second
)

func probeRTT(sl *Slot, f Family) (bool, time.Duration) {
	ip := sl.ip(f)
	if ip == nil {
		return false, 0
	}
	network := "tcp4"
	if f == Fam6 {
		network = "tcp6"
	}
	for _, t := range probeTargets[f] {
		d := net.Dialer{LocalAddr: &net.TCPAddr{IP: ip}, Control: nics.ForceEgress(sl.Index, f == Fam6), Timeout: probeTimeout}
		start := time.Now()
		if c, err := d.Dial(network, t); err == nil {
			rtt := time.Since(start)
			c.Close()
			return true, rtt
		}
	}
	return false, 0
}

func probe(sl *Slot, f Family) bool {
	ok, _ := probeRTT(sl, f)
	return ok
}

type probeJob struct {
	sl *Slot
	f  Family
}

// RunProbes checks every adapter's internet access per address family.
func (s *Scheduler) RunProbes(stop <-chan struct{}) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		for _, j := range s.probeDue() {
			go func() {
				defer j.sl.probing[j.f].Store(false)
				up, rtt := probeRTT(j.sl, j.f)
				s.SetStateWithRTT(j.sl, j.f, up, rtt)
			}()
		}
	}
}

// probeDue returns the (slot, family) pairs that need a probe now and marks them in flight.
func (s *Scheduler) probeDue() []probeJob {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	var due []probeJob
	for _, sl := range s.slots {
		if !sl.present {
			continue
		}
		for _, f := range []Family{Fam4, Fam6} {
			if sl.ip(f) == nil || sl.probing[f].Load() {
				continue
			}
			every := probeEveryUp
			switch sl.state[f] {
			case stateUnknown:
				every = 0
			case stateDown:
				every = probeEveryDown
			}
			if now.Sub(sl.lastProbe[f]) >= every {
				sl.lastProbe[f] = now
				sl.probing[f].Store(true)
				due = append(due, probeJob{sl, f})
			}
		}
	}
	return due
}
