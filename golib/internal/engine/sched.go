package engine

import (
	"io"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"netbond-android/internal/nics"
)

// Family selects which address of a NIC a flow is pinned to.
type Family int

const (
	Fam4 Family = iota
	Fam6
)

// linkState is the result of the internet probe.
type linkState int

const (
	stateUnknown linkState = iota
	stateUp
	stateDown
)

func (l linkState) String() string {
	return [...]string{"checking", "up", "no internet"}[l]
}

// Mode selects the flow bonding strategy.
type Mode int

const (
	ModeTurboBoost    Mode = 0 // Max aggregate throughput: dynamic proportional balancing based on live EWMA capacity
	ModeSmartAdaptive Mode = 1 // QoS: low-latency/jitter (DNS/VoIP/Gaming) to lowest RTT; bulk to proportional
	ModeDataSaver     Mode = 2 // Wi-Fi preferred; Cellular only on failover / overflow
	ModeSplitEvenly   Mode = 3 // 50/50 symmetric round-robin
)

type slotAddr struct{ ip4, ip6 net.IP }

type rateSample struct{ dt, in, out float64 }

// Slot is a NIC plus its live counters. Counters are atomic so the data path never takes a lock.
type Slot struct {
	Name  string
	Index int
	key   string
	addr  atomic.Pointer[slotAddr]

	active, total, failed atomic.Int64 // active is across families (idle check on removal)
	actF                  [2]atomic.Int64
	bytesIn, bytesOut     [2]atomic.Int64
	rttMs                 [2]atomic.Int64 // observed RTT in ms
	probing               [2]atomic.Bool

	// guarded by Scheduler.mu
	handle          uint64
	weight          [2]float64
	base            float64 // Android port: the configured weight, used until speeds are measured
	present         bool
	state           [2]linkState
	lastProbe       [2]time.Time
	downUntil       [2]time.Time
	lastIn, lastOut [2]int64
	rateIn, rateOut [2]float64 // bytes/s over a 1-second sliding window
	rateHist        [2][8]rateSample
	rateIdx         [2]int
	rateLen         [2]int
	ewmaIn, ewmaOut [2]float64 // EWMA smoothed throughput (bytes/s)
	peak            [2]float64 // observed capacity: max recent rateIn+rateOut
	peakUp          [2]float64 // Android port: the same, for upload (rateOut) alone
	tcpDeficit       [2]float64 // smooth weighted round-robin deficit for TCP flows
	udpDeficit       [2]float64 // smooth weighted round-robin deficit for UDP/QUIC

	// The adapter's total traffic from the OS's counters (all apps, not only net-merge's flows).
	linkIn, linkOut         uint64
	linkRateIn, linkRateOut float64
	linkPrimed              bool

	// Android: open flows on this slot, so they can be closed the moment its network goes away
	// (see CloseGone). Keys are io.Closer.
	conns sync.Map
}

func (sl *Slot) ip(f Family) net.IP {
	a := sl.addr.Load()
	if a == nil {
		return nil
	}
	if f == Fam6 {
		return a.ip6
	}
	return a.ip4
}

func (sl *Slot) isWifi() bool {
	lower := strings.ToLower(sl.Name)
	return strings.Contains(lower, "wi-fi") || strings.Contains(lower, "wlan")
}

type SlotStats struct {
	Name              string
	IP                string
	IP6               string
	State             string // IPv4 probe state, "-" if the adapter has no IPv4
	State6            string
	Weight, Weight6   float64
	Present           bool
	Active, Total     int64
	Failed            int64
	BytesIn, BytesOut int64
	RateIn, RateOut   float64 // through net-merge, both families summed (bytes/s)
	LinkIn, LinkOut   float64 // through the adapter by any app, from the OS's counters (bytes/s)
	Peak, Peak6       float64
	RTT, RTT6         int64
}

// Scheduler selects the best NIC for each outbound connection.
type Scheduler struct {
	mu        sync.Mutex
	slots     []*Slot
	mode      Mode
	now       func() time.Time
	uploading bool
	rrSeq     uint64
}

func NewScheduler(nics []nics.Nic) *Scheduler {
	s := &Scheduler{now: time.Now, mode: ModeTurboBoost}
	s.Update(nics)
	return s
}

func (s *Scheduler) SetAuto(on bool) {
	s.mu.Lock()
	m := ModeSplitEvenly
	if on {
		m = ModeTurboBoost
	}
	changed := s.mode != m
	s.mode = m
	s.applyModeWeightsLocked(changed)
	s.mu.Unlock()
}

func (s *Scheduler) SetMode(m Mode) {
	s.mu.Lock()
	changed := s.mode != m
	s.mode = m
	s.applyModeWeightsLocked(changed)
	s.mu.Unlock()
}

func (s *Scheduler) Mode() Mode {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mode
}

func (s *Scheduler) ResetSession() {
	s.mu.Lock()
	for _, sl := range s.slots {
		sl.downUntil = [2]time.Time{}
		sl.rateHist = [2][8]rateSample{}
		sl.rateIdx = [2]int{}
		sl.rateLen = [2]int{}
		sl.rateIn = [2]float64{}
		sl.rateOut = [2]float64{}
		sl.tcpDeficit = [2]float64{}
		sl.udpDeficit = [2]float64{}
	}
	s.applyModeWeightsLocked(true)
	s.mu.Unlock()
}

func (s *Scheduler) applyModeWeightsLocked(resetDeficits bool) {
	if resetDeficits {
		for _, sl := range s.slots {
			sl.tcpDeficit = [2]float64{}
			sl.udpDeficit = [2]float64{}
			sl.downUntil = [2]time.Time{}
		}
	}
	switch s.mode {
	case ModeSplitEvenly:
		for _, sl := range s.slots {
			sl.weight = [2]float64{1.0, 1.0}
		}
	case ModeDataSaver:
		for _, f := range []Family{Fam4, Fam6} {
			var hasUpWifi bool
			for _, sl := range s.slots {
				if sl.present && sl.ip(f) != nil && sl.isWifi() && sl.state[f] != stateDown {
					hasUpWifi = true
					break
				}
			}
			for _, sl := range s.slots {
				if !sl.present || sl.ip(f) == nil {
					continue
				}
				if hasUpWifi {
					if sl.isWifi() {
						sl.weight[f] = 1.0
					} else {
						sl.weight[f] = 0.01
					}
				} else {
					sl.weight[f] = 1.0
				}
			}
		}
	case ModeTurboBoost, ModeSmartAdaptive:
		for _, f := range []Family{Fam4, Fam6} {
			var maxUp, maxCap float64
			live, measured := 0, true
			for _, sl := range s.slots {
				if !sl.present || sl.ip(f) == nil {
					continue
				}
				live++
				maxUp = math.Max(maxUp, sl.peakUp[f])
				curCap := math.Max(sl.ewmaIn[f]+sl.ewmaOut[f], sl.peak[f])
				maxCap = math.Max(maxCap, curCap)
				if sl.peak[f] < minMeasuredPeak {
					measured = false
				}
			}
			upMeasured := maxUp > 0
			for _, sl := range s.slots {
				if sl.present && sl.ip(f) != nil && sl.peakUp[f] < minMeasuredPeak/2 {
					upMeasured = false
				}
			}
			for _, sl := range s.slots {
				if !sl.present || sl.ip(f) == nil {
					continue
				}
				switch {
				case live < 2 || !measured:
					w := sl.base
					if w <= 0 {
						w = 1.0
					}
					sl.weight[f] = w
				case s.uploading && upMeasured:
					sl.weight[f] = math.Max(0.05, sl.peakUp[f]/maxUp)
				default:
					cap := math.Max(sl.ewmaIn[f]+sl.ewmaOut[f], sl.peak[f])
					if maxCap > 0 {
						sl.weight[f] = math.Max(0.05, cap/maxCap)
					} else {
						sl.weight[f] = 1.0
					}
				}
			}
		}
	}
}

func normAddr(n nics.Nic) *slotAddr {
	a := &slotAddr{}
	if n.IP != nil {
		a.ip4 = n.IP.To4()
	}
	if n.IP6 != nil && n.IP6.To4() == nil {
		a.ip6 = n.IP6
	}
	return a
}

func (s *Scheduler) Update(nics []nics.Nic) {
	var staleConns []io.Closer
	s.mu.Lock()
	for _, sl := range s.slots {
		sl.present = false
	}
	for _, n := range nics {
		na := normAddr(n)
		var found *Slot
		for _, sl := range s.slots {
			if sl.key == n.Name {
				found = sl
				break
			}
		}
		if found == nil {
			initWeight := n.Weight
			if s.mode == ModeSplitEvenly {
				initWeight = 1.0
			}
			sl := &Slot{
				Name:    n.Name,
				Index:   n.Index,
				key:     n.Name,
				handle:  n.Handle,
				weight:  [2]float64{initWeight, initWeight},
				base:    n.Weight,
				present: true,
			}
			sl.addr.Store(na)
			s.slots = append(s.slots, sl)
			continue
		}
		found.present = true
		found.base = n.Weight
		if s.mode == ModeSplitEvenly {
			found.weight = [2]float64{1.0, 1.0}
		}
		if n.Handle != 0 && found.handle != 0 && found.handle != n.Handle {
			found.handle = n.Handle
			found.state = [2]linkState{stateUnknown, stateUnknown}
			found.downUntil = [2]time.Time{}
			found.rttMs[Fam4].Store(0)
			found.rttMs[Fam6].Store(0)
			found.tcpDeficit = [2]float64{}
			found.udpDeficit = [2]float64{}
			found.peak = [2]float64{}
			found.peakUp = [2]float64{}
			found.ewmaIn = [2]float64{}
			found.ewmaOut = [2]float64{}
			found.lastProbe = [2]time.Time{}
			found.conns.Range(func(k, _ any) bool {
				staleConns = append(staleConns, k.(io.Closer))
				found.conns.Delete(k)
				return true
			})
		} else if found.handle == 0 {
			found.handle = n.Handle
		}
		old := found.addr.Load()
		if old == nil || !old.ip4.Equal(na.ip4) {
			found.state[Fam4] = stateUnknown
		}
		if old == nil || !old.ip6.Equal(na.ip6) {
			found.state[Fam6] = stateUnknown
		}
		if old == nil || !old.ip4.Equal(na.ip4) || !old.ip6.Equal(na.ip6) {
			found.addr.Store(na)
		}
	}
	kept := s.slots[:0]
	for _, sl := range s.slots {
		if sl.present || sl.active.Load() > 0 {
			kept = append(kept, sl)
		}
	}
	s.slots = kept
	s.applyModeWeightsLocked(false)
	s.mu.Unlock()

	for _, c := range staleConns {
		_ = c.Close()
	}
}

const minMeasuredPeak = 50_000 // bytes/s (0.4 Mbit/s)

func (s *Scheduler) Tick(dt time.Duration) {
	s.mu.Lock()
	slots := append([]*Slot(nil), s.slots...)
	s.mu.Unlock()

	type sample struct {
		in, out uint64
		ok      bool
	}

	samples := make([]sample, len(slots))
	for i, sl := range slots {
		in, out, ok := nics.Octets(sl.Index)
		samples[i] = sample{in, out, ok}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	sec := dt.Seconds()
	if sec <= 0 {
		sec = 1.0
	}
	for i, sl := range slots {
		in, out, ok := samples[i].in, samples[i].out, samples[i].ok
		if !sl.present || !ok {
			sl.linkRateIn, sl.linkRateOut, sl.linkPrimed = 0, 0, false
			continue
		}
		if sl.linkPrimed && in >= sl.linkIn && out >= sl.linkOut {
			sl.linkRateIn = float64(in-sl.linkIn) / sec
			sl.linkRateOut = float64(out-sl.linkOut) / sec
		}
		sl.linkIn, sl.linkOut, sl.linkPrimed = in, out, true
	}

	alpha := 1.0 - math.Exp(-sec/2.0)
	ewmaDecay := math.Pow(0.90, sec)
	peakDecay := math.Pow(0.985, sec)
	idleGrow := math.Pow(1.03, sec)

	var totIn, totOut float64
	for _, f := range []Family{Fam4, Fam6} {
		for _, sl := range s.slots {
			in, out := sl.bytesIn[f].Load(), sl.bytesOut[f].Load()
			dIn := float64(in - sl.lastIn[f])
			dOut := float64(out - sl.lastOut[f])
			sl.lastIn[f], sl.lastOut[f] = in, out

			idx := sl.rateIdx[f]
			sl.rateHist[f][idx] = rateSample{dt: sec, in: dIn, out: dOut}
			sl.rateIdx[f] = (idx + 1) % len(sl.rateHist[f])
			if sl.rateLen[f] < len(sl.rateHist[f]) {
				sl.rateLen[f]++
			}

			var winSec, winIn, winOut float64
			for k := 0; k < sl.rateLen[f]; k++ {
				pos := (idx - k + len(sl.rateHist[f])) % len(sl.rateHist[f])
				rs := sl.rateHist[f][pos]
				winSec += rs.dt
				winIn += rs.in
				winOut += rs.out
				if winSec >= 0.99 {
					break
				}
			}
			if winSec <= 0 {
				winSec = sec
			}
			sl.rateIn[f] = winIn / winSec
			sl.rateOut[f] = winOut / winSec
			totIn += sl.rateIn[f]
			totOut += sl.rateOut[f]
		}
	}

	switch {
	case totOut > minMeasuredPeak && totOut > 1.5*totIn:
		s.uploading = true
	case totIn > 1.5*totOut:
		s.uploading = false
	}

	for _, f := range []Family{Fam4, Fam6} {
		var maxPeak, maxUp, maxCap float64
		live, measured := 0, true
		idle := map[*Slot]bool{}
		for _, sl := range s.slots {
			curIn := sl.rateIn[f]
			curOut := sl.rateOut[f]

			if curIn > 0 {
				if sl.ewmaIn[f] <= 0 {
					sl.ewmaIn[f] = curIn
				} else {
					sl.ewmaIn[f] = alpha*curIn + (1.0-alpha)*sl.ewmaIn[f]
				}
			} else {
				sl.ewmaIn[f] *= ewmaDecay
			}

			if curOut > 0 {
				if sl.ewmaOut[f] <= 0 {
					sl.ewmaOut[f] = curOut
				} else {
					sl.ewmaOut[f] = alpha*curOut + (1.0-alpha)*sl.ewmaOut[f]
				}
			} else {
				sl.ewmaOut[f] *= ewmaDecay
			}

			r := curIn + curOut
			if r >= sl.peak[f] {
				sl.peak[f] = r
			} else if r >= 0.25*sl.peak[f] {
				sl.peak[f] = math.Max(r, sl.peak[f]*peakDecay)
			} else if r < 0.05*sl.peak[f] {
				idle[sl] = true
			}
			if curOut >= sl.peakUp[f] {
				sl.peakUp[f] = curOut
			} else if curOut >= 0.25*sl.peakUp[f] {
				sl.peakUp[f] = math.Max(curOut, sl.peakUp[f]*peakDecay)
			}
			if !sl.present || sl.ip(f) == nil {
				continue
			}
			live++
			maxPeak = math.Max(maxPeak, sl.peak[f])
			maxUp = math.Max(maxUp, sl.peakUp[f])
			curCap := math.Max(sl.ewmaIn[f]+sl.ewmaOut[f], sl.peak[f])
			maxCap = math.Max(maxCap, curCap)
		}

		for _, sl := range s.slots {
			if sl.present && sl.ip(f) != nil && sl.peakUp[f] > 0 && sl.rateOut[f] < 0.05*sl.peakUp[f] {
				sl.peakUp[f] = math.Min(sl.peakUp[f]*idleGrow, math.Max(sl.peakUp[f], 0.5*maxUp))
			}
		}
		for sl := range idle {
			if sl.present && sl.ip(f) != nil && sl.peak[f] > 0 {
				sl.peak[f] = math.Min(sl.peak[f]*idleGrow, math.Max(sl.peak[f], 0.5*maxPeak))
			}
		}
		for _, sl := range s.slots {
			if sl.present && sl.ip(f) != nil && sl.peak[f] < minMeasuredPeak {
				measured = false
			}
		}
		upMeasured := maxUp > 0
		for _, sl := range s.slots {
			if sl.present && sl.ip(f) != nil && sl.peakUp[f] < minMeasuredPeak/2 {
				upMeasured = false
			}
		}

		switch s.mode {
		case ModeSplitEvenly:
			for _, sl := range s.slots {
				sl.weight[f] = 1.0
			}

		case ModeDataSaver:
			var hasUpWifi bool
			for _, sl := range s.slots {
				if sl.present && sl.ip(f) != nil && sl.isWifi() && sl.state[f] != stateDown {
					hasUpWifi = true
					break
				}
			}
			for _, sl := range s.slots {
				if !sl.present || sl.ip(f) == nil {
					continue
				}
				if hasUpWifi {
					if sl.isWifi() {
						sl.weight[f] = 1.0
					} else {
						sl.weight[f] = 0.01
					}
				} else {
					sl.weight[f] = 1.0
				}
			}

		case ModeTurboBoost, ModeSmartAdaptive:
			for _, sl := range s.slots {
				if !sl.present || sl.ip(f) == nil {
					continue
				}
				switch {
				case live < 2 || !measured:
					sl.weight[f] = sl.base
					if sl.weight[f] <= 0 {
						sl.weight[f] = 1.0
					}
				case s.uploading && upMeasured:
					sl.weight[f] = math.Max(0.05, sl.peakUp[f]/maxUp)
				default:
					cap := math.Max(sl.ewmaIn[f]+sl.ewmaOut[f], sl.peak[f])
					if maxCap > 0 {
						sl.weight[f] = math.Max(0.05, cap/maxCap)
					} else {
						sl.weight[f] = 1.0
					}
				}
			}
		}
	}
}

// IsLatencySensitive checks if a flow is interactive or latency-critical (DNS, VoIP, Gaming, SSH).
func IsLatencySensitive(port int, isUDP bool) bool {
	if isUDP {
		// Bulk HTTP/3 (QUIC) on 443/80 should bond proportionally; DNS/VoIP/gaming/STUN pin to lowest RTT.
		return port != 443 && port != 80
	}
	switch port {
	case 53, 853: // DNS, DoT
		return true
	case 123: // NTP
		return true
	case 5060, 5061: // SIP/VoIP
		return true
	case 5222, 5223, 5228: // Push notifications (FCM / XMPP)
		return true
	case 22: // SSH interactive
		return true
	}
	return false
}

func (s *Scheduler) Acquire(skip map[string]struct{}) *Slot {
	return s.AcquireForPort(Fam4, 0, false, skip)
}

func (s *Scheduler) AcquireFor(f Family, skip map[string]struct{}) *Slot {
	return s.AcquireForPort(f, 0, false, skip)
}

func (s *Scheduler) AcquireForPort(f Family, port int, isUDP bool, skip map[string]struct{}) *Slot {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()

	// Tiers: 0 = probed up, 1 = not probed yet, 2 = probe failed or link error cooldown.
	var tiers [3][]*Slot
	for _, sl := range s.slots {
		if !sl.present || sl.ip(f) == nil {
			continue
		}
		if _, no := skip[sl.key]; no {
			continue
		}
		t := 1
		switch {
		case now.Before(sl.downUntil[f]) || sl.state[f] == stateDown:
			t = 2
		case sl.state[f] == stateUp:
			t = 0
		}
		tiers[t] = append(tiers[t], sl)
	}

	var pool []*Slot
	for _, tier := range tiers {
		if len(tier) > 0 {
			pool = tier
			break
		}
	}
	if len(pool) == 0 {
		return nil
	}

	// Strict Data Saver: stay on Wi-Fi whenever a healthy Wi-Fi link is in the active pool.
	if s.mode == ModeDataSaver && len(pool) > 1 {
		var wifiPool []*Slot
		for _, sl := range pool {
			if sl.isWifi() {
				wifiPool = append(wifiPool, sl)
			}
		}
		if len(wifiPool) > 0 {
			pool = wifiPool
		}
	}

	// QoS optimization for Smart Adaptive mode: steer latency-sensitive traffic to lowest RTT
	if s.mode == ModeSmartAdaptive && IsLatencySensitive(port, isUDP) && len(pool) > 1 {
		otherFam := Fam6
		if f == Fam6 {
			otherFam = Fam4
		}
		var bestRTT *Slot
		lowestRTT := int64(math.MaxInt64)
		for _, sl := range pool {
			rtt := sl.rttMs[f].Load()
			if rtt <= 0 {
				rtt = sl.rttMs[otherFam].Load()
			}
			if rtt > 0 && rtt < lowestRTT {
				lowestRTT = rtt
				bestRTT = sl
			}
		}
		if bestRTT != nil {
			bestRTT.active.Add(1)
			bestRTT.actF[f].Add(1)
			bestRTT.total.Add(1)
			return bestRTT
		}
	}

	start := int(s.rrSeq % uint64(len(pool)))

	// Smooth Weighted Round-Robin (SWRR) for UDP/QUIC flows so HTTP/3 bonds proportionally.
	if isUDP {
		var totalW float64
		for _, sl := range pool {
			w := sl.weight[f]
			if w <= 0 {
				w = 1.0
			}
			if (s.mode == ModeTurboBoost || s.mode == ModeSmartAdaptive) && sl.peak[f] < minMeasuredPeak {
				w = math.Max(w, 0.5)
			}
			sl.udpDeficit[f] += w
			totalW += w
		}
		var best *Slot
		bestDef := math.Inf(-1)
		for i := range pool {
			sl := pool[(start+i)%len(pool)]
			if sl.udpDeficit[f] > bestDef {
				best, bestDef = sl, sl.udpDeficit[f]
			}
		}
		best.udpDeficit[f] -= totalW
		s.rrSeq++
		best.active.Add(1)
		best.actF[f].Add(1)
		best.total.Add(1)
		return best
	}

	// Smooth Weighted Round-Robin (SWRR) for TCP flows: strictly proportional and immune
	// to idle background keep-alive sockets sitting in actF.
	var totalW float64
	for _, sl := range pool {
		w := sl.weight[f]
		if w <= 0 {
			w = 1.0
		}
		if (s.mode == ModeTurboBoost || s.mode == ModeSmartAdaptive) && sl.peak[f] < minMeasuredPeak {
			w = math.Max(w, 0.5)
		}
		sl.tcpDeficit[f] += w
		totalW += w
	}
	var best *Slot
	bestDef := math.Inf(-1)
	for i := range pool {
		sl := pool[(start+i)%len(pool)]
		if sl.tcpDeficit[f] > bestDef {
			best, bestDef = sl, sl.tcpDeficit[f]
		}
	}
	best.tcpDeficit[f] -= totalW
	s.rrSeq++
	best.active.Add(1)
	best.actF[f].Add(1)
	best.total.Add(1)
	return best
}

func (s *Scheduler) Release(sl *Slot) { s.ReleaseFor(sl, Fam4) }

func (s *Scheduler) ReleaseFor(sl *Slot, f Family) {
	sl.active.Add(-1)
	sl.actF[f].Add(-1)
}

func (s *Scheduler) V6Usable() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sl := range s.slots {
		if sl.present && sl.ip(Fam6) != nil && sl.state[Fam6] != stateDown {
			return true
		}
	}
	return false
}

func (s *Scheduler) V6CoversAll() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	any6 := false
	for _, sl := range s.slots {
		if !sl.present {
			continue
		}
		has6 := sl.ip(Fam6) != nil && sl.state[Fam6] != stateDown
		if has6 {
			any6 = true
		}
		if sl.ip(Fam4) != nil && sl.state[Fam4] != stateDown && !has6 {
			return false
		}
	}
	return any6
}

func (s *Scheduler) SetState(sl *Slot, f Family, up bool) {
	s.SetStateWithRTT(sl, f, up, 0)
}

func (s *Scheduler) SetStateWithRTT(sl *Slot, f Family, up bool, rtt time.Duration) {
	s.SetStateForHandle(sl, 0, f, up, rtt)
}

func (s *Scheduler) SetStateForHandle(sl *Slot, expectedHandle uint64, f Family, up bool, rtt time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if expectedHandle != 0 && sl.handle != expectedHandle {
		return
	}
	if up {
		sl.state[f] = stateUp
		sl.downUntil[f] = time.Time{}
		if rtt > 0 {
			sl.rttMs[f].Store(rtt.Milliseconds())
		}
	} else {
		sl.state[f] = stateDown
	}
}

func (s *Scheduler) Fail(sl *Slot, f Family, cooldown time.Duration) {
	s.mu.Lock()
	sl.downUntil[f] = s.now().Add(cooldown)
	s.mu.Unlock()
	sl.failed.Add(1)
}

func (s *Scheduler) CloseGone() int {
	s.mu.Lock()
	var gone []*Slot
	for _, sl := range s.slots {
		if !sl.present {
			gone = append(gone, sl)
		}
	}
	s.mu.Unlock()
	n := 0
	for _, sl := range gone {
		sl.conns.Range(func(k, _ any) bool {
			k.(io.Closer).Close()
			n++
			return true
		})
	}
	return n
}

func (s *Scheduler) Snapshot() []SlotStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]SlotStats, 0, len(s.slots))
	for _, sl := range s.slots {
		a := sl.addr.Load()
		st := SlotStats{
			Name: sl.Name, State: "-", State6: "-", Present: sl.present,
			Weight: sl.weight[Fam4], Weight6: sl.weight[Fam6],
			Active: sl.active.Load(), Total: sl.total.Load(), Failed: sl.failed.Load(),
			BytesIn:  sl.bytesIn[Fam4].Load() + sl.bytesIn[Fam6].Load(),
			BytesOut: sl.bytesOut[Fam4].Load() + sl.bytesOut[Fam6].Load(),
			RateIn:   sl.rateIn[Fam4] + sl.rateIn[Fam6], RateOut: sl.rateOut[Fam4] + sl.rateOut[Fam6],
			LinkIn:   sl.linkRateIn, LinkOut: sl.linkRateOut,
			Peak:     sl.peak[Fam4], Peak6: sl.peak[Fam6],
			RTT:      sl.rttMs[Fam4].Load(), RTT6: sl.rttMs[Fam6].Load(),
		}
		if a != nil {
			if a.ip4 != nil {
				st.IP, st.State = a.ip4.String(), sl.state[Fam4].String()
			}
			if a.ip6 != nil {
				st.IP6, st.State6 = a.ip6.String(), sl.state[Fam6].String()
			}
		}
		out = append(out, st)
	}
	return out
}
