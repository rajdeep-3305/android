package engine

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"netbond-android/internal/nics"
)

const hundredMB = 100 << 20 // 104,857,600 bytes

// startRateLimitedEchoServer starts a TCP server that enforces per-link bandwidth ceilings
// based on the client source IP (wifiIP vs cellIP). Protocol:
//   - Client sends 1-byte op ('D' = download N bytes, 'U' = upload N bytes) + 8-byte uint64 length.
//   - For 'D', server streams N deterministic pattern bytes at the link's rate cap.
//   - For 'U', server reads and verifies N bytes at the link's rate cap, then writes 1-byte 'K'.
type linkPacer struct {
	mu    sync.Mutex
	start time.Time
	moved int64
	rate  float64
}

func (p *linkPacer) pace(n int) {
	if p.rate <= 0 || n <= 0 {
		return
	}
	p.mu.Lock()
	if p.start.IsZero() {
		p.start = time.Now()
	}
	p.moved += int64(n)
	expected := time.Duration(float64(p.moved) / p.rate * float64(time.Second))
	sleepFor := expected - time.Since(p.start)
	p.mu.Unlock()
	if sleepFor > time.Millisecond {
		time.Sleep(sleepFor)
	}
}

func startRateLimitedEchoServer(
	t *testing.T,
	wifiIP, cellIP string,
	wifiRateBps, cellRateBps float64,
) (host string, port int, stopFn func()) {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h, pStr, _ := net.SplitHostPort(ln.Addr().String())
	p, _ := strconv.Atoi(pStr)

	wifiPacer := &linkPacer{rate: wifiRateBps}
	cellPacer := &linkPacer{rate: cellRateBps}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func(conn net.Conn) {
				defer wg.Done()
				defer conn.Close()
				remoteHost, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
				pacer := wifiPacer
				if remoteHost == cellIP {
					pacer = cellPacer
				}

				var hdr [9]byte
				if _, err := io.ReadFull(conn, hdr[:]); err != nil {
					return
				}
				op := hdr[0]
				n := int64(binary.BigEndian.Uint64(hdr[1:]))

				buf := make([]byte, 64<<10)
				for i := range buf {
					buf[i] = byte(i)
				}

				var moved int64
				switch op {
				case 'D':
					for moved < n {
						chunk := len(buf)
						if rem := n - moved; rem < int64(chunk) {
							chunk = int(rem)
						}
						wn, err := conn.Write(buf[:chunk])
						moved += int64(wn)
						if err != nil {
							return
						}
						pacer.pace(wn)
					}
				case 'U':
					for moved < n {
						chunk := len(buf)
						if rem := n - moved; rem < int64(chunk) {
							chunk = int(rem)
						}
						rn, err := io.ReadFull(conn, buf[:chunk])
						moved += int64(rn)
						if err != nil {
							return
						}
						pacer.pace(rn)
					}
					_, _ = conn.Write([]byte{'K'})
				}
			}(c)
		}
	}()

	return h, p, func() {
		ln.Close()
		wg.Wait()
	}
}

func startTicker(s *Scheduler) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		last := time.Now()
		for {
			select {
			case <-stop:
				return
			case now := <-t.C:
				s.Tick(now.Sub(last))
				last = now
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

func runDownloadBatch(t *testing.T, b *Balancer, host string, port int, totalBytes int64, streams int) {
	t.Helper()
	perStream := totalBytes / int64(streams)
	remainder := totalBytes % int64(streams)

	var gotTotal atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < streams; i++ {
		want := perStream
		if i == 0 {
			want += remainder
		}
		wg.Add(1)
		go func(n int64) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			c, err := b.DialPinned(ctx, host, port)
			if err != nil {
				t.Errorf("DialPinned download: %v", err)
				return
			}
			defer c.Close()

			var hdr [9]byte
			hdr[0] = 'D'
			binary.BigEndian.PutUint64(hdr[1:], uint64(n))
			if _, err := c.Write(hdr[:]); err != nil {
				t.Errorf("write header: %v", err)
				return
			}

			buf := make([]byte, 64<<10)
			var readN int64
			for readN < n {
				rn, err := c.Read(buf)
				if rn > 0 {
					for j := 0; j < rn; j++ {
						exp := byte((readN + int64(j)) % (64 << 10))
						if buf[j] != exp {
							t.Errorf("corrupt byte at %d: got %d want %d", readN+int64(j), buf[j], exp)
							return
						}
					}
					readN += int64(rn)
				}
				if err != nil {
					if err == io.EOF {
						break
					}
					t.Errorf("read: %v", err)
					return
				}
			}
			gotTotal.Add(readN)
		}(want)
	}
	wg.Wait()
	if gotTotal.Load() != totalBytes {
		t.Fatalf("download total=%d, want %d", gotTotal.Load(), totalBytes)
	}
}

func runUploadBatch(t *testing.T, b *Balancer, host string, port int, totalBytes int64, streams int) {
	t.Helper()
	perStream := totalBytes / int64(streams)
	remainder := totalBytes % int64(streams)

	var sentTotal atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < streams; i++ {
		size := perStream
		if i == 0 {
			size += remainder
		}
		wg.Add(1)
		go func(n int64) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			c, err := b.DialPinned(ctx, host, port)
			if err != nil {
				t.Errorf("DialPinned upload: %v", err)
				return
			}
			defer c.Close()

			var hdr [9]byte
			hdr[0] = 'U'
			binary.BigEndian.PutUint64(hdr[1:], uint64(n))
			if _, err := c.Write(hdr[:]); err != nil {
				t.Errorf("write header: %v", err)
				return
			}

			payload := make([]byte, 64<<10)
			for j := range payload {
				payload[j] = byte(j)
			}
			var sent int64
			for sent < n {
				chunk := len(payload)
				if rem := n - sent; rem < int64(chunk) {
					chunk = int(rem)
				}
				wn, err := c.Write(payload[:chunk])
				sent += int64(wn)
				if err != nil {
					t.Errorf("write payload: %v", err)
					return
				}
			}
			var ack [1]byte
			if _, err := io.ReadFull(c, ack[:]); err != nil || ack[0] != 'K' {
				t.Errorf("upload ack err=%v ack=%v", err, ack)
				return
			}
			sentTotal.Add(sent)
		}(size)
	}
	wg.Wait()
	if sentTotal.Load() != totalBytes {
		t.Fatalf("upload total=%d, want %d", sentTotal.Load(), totalBytes)
	}
}

func Test100MB_TurboBoost_DownloadAndUpload(t *testing.T) {
	needLoopbackAliases(t)
	host, port, stopSrv := startRateLimitedEchoServer(t, "127.0.0.2", "127.0.0.3", 40<<20, 120<<20)
	defer stopSrv()

	s := NewScheduler([]nics.Nic{nic("Wi-Fi", "127.0.0.2", 1), nic("Mobile data", "127.0.0.3", 1)})
	s.SetMode(ModeTurboBoost)
	w, c := s.slots[0], s.slots[1]
	s.SetStateWithRTT(w, Fam4, true, 12*time.Millisecond)
	s.SetStateWithRTT(c, Fam4, true, 40*time.Millisecond)
	b := NewBalancer(s)
	stopTick := startTicker(s)
	defer stopTick()

	// Wave 1 (20 MB) measures link capacities, Wave 2 (80 MB) bonds proportionally.
	runDownloadBatch(t, b, host, port, 20<<20, 8)
	runDownloadBatch(t, b, host, port, 80<<20, 16)

	snap := s.Snapshot()
	wifiIn, cellIn := snap[0].BytesIn, snap[1].BytesIn
	t.Logf("TurboBoost 100MB Download: Wi-Fi=%d MB, Mobile=%d MB, active=%d/%d",
		wifiIn>>20, cellIn>>20, snap[0].Active, snap[1].Active)
	if wifiIn+cellIn < hundredMB {
		t.Fatalf("expected >= %d bytesIn, got %d", hundredMB, wifiIn+cellIn)
	}
	if wifiIn < 10<<20 || cellIn < 55<<20 {
		t.Fatalf("TurboBoost 100MB download not proportional (1:3): wifi=%d cell=%d", wifiIn, cellIn)
	}
	if snap[0].Active != 0 || snap[1].Active != 0 {
		t.Fatalf("leaked active slots after download: %+v", snap)
	}

	// Upload 100 MB (20 MB warmup + 80 MB proportional)
	runUploadBatch(t, b, host, port, 20<<20, 8)
	runUploadBatch(t, b, host, port, 80<<20, 16)

	snap = s.Snapshot()
	wifiOut, cellOut := snap[0].BytesOut, snap[1].BytesOut
	t.Logf("TurboBoost 100MB Upload: Wi-Fi=%d MB, Mobile=%d MB, active=%d/%d",
		wifiOut>>20, cellOut>>20, snap[0].Active, snap[1].Active)
	if wifiOut+cellOut < hundredMB {
		t.Fatalf("expected >= %d bytesOut, got %d", hundredMB, wifiOut+cellOut)
	}
	if wifiOut < 10<<20 || cellOut < 55<<20 {
		t.Fatalf("TurboBoost 100MB upload not proportional (1:3): wifi=%d cell=%d", wifiOut, cellOut)
	}
	if snap[0].Active != 0 || snap[1].Active != 0 {
		t.Fatalf("leaked active slots after upload: %+v", snap)
	}
}

func Test100MB_SmartAdaptive_BulkAndQoS(t *testing.T) {
	needLoopbackAliases(t)
	host, port, stopSrv := startRateLimitedEchoServer(t, "127.0.0.2", "127.0.0.3", 40<<20, 120<<20)
	defer stopSrv()

	s := NewScheduler([]nics.Nic{nic("Wi-Fi", "127.0.0.2", 1), nic("Mobile data", "127.0.0.3", 1)})
	s.SetMode(ModeSmartAdaptive)
	w, c := s.slots[0], s.slots[1]
	s.SetStateWithRTT(w, Fam4, true, 8*time.Millisecond)
	s.SetStateWithRTT(c, Fam4, true, 45*time.Millisecond)
	b := NewBalancer(s)
	stopTick := startTicker(s)
	defer stopTick()

	runDownloadBatch(t, b, host, port, 20<<20, 8)
	runDownloadBatch(t, b, host, port, 80<<20, 16)

	// Verify QoS: DNS/VoIP/Push pin 100% to lowest RTT (Wi-Fi), while HTTP/3 QUIC (UDP 443) bonds
	for _, p := range []int{53, 3478, 5228} {
		for i := 0; i < 10; i++ {
			sl := s.AcquireForPort(Fam4, p, p != 5228, nil)
			s.ReleaseFor(sl, Fam4)
			if sl != w {
				t.Fatalf("SmartAdaptive QoS port %d went to %s, want Wi-Fi", p, sl.Name)
			}
		}
	}
	quicWifi, quicCell := 0, 0
	for i := 0; i < 20; i++ {
		pc, err := b.ListenUDP(context.Background(), Fam4, 443)
		if err != nil {
			t.Fatalf("ListenUDP 443: %v", err)
		}
		addr := pc.LocalAddr().String()
		pc.Close()
		if strings.HasPrefix(addr, "127.0.0.2:") {
			quicWifi++
		} else if strings.HasPrefix(addr, "127.0.0.3:") {
			quicCell++
		}
	}
	snap := s.Snapshot()
	t.Logf("SmartAdaptive 100MB: Wi-Fi=%d MB, Mobile=%d MB, QUIC443(wifi=%d,cell=%d), active=%d/%d",
		snap[0].BytesIn>>20, snap[1].BytesIn>>20, quicWifi, quicCell, snap[0].Active, snap[1].Active)
	if snap[0].BytesIn < 10<<20 || snap[1].BytesIn < 55<<20 {
		t.Fatalf("SmartAdaptive 100MB bulk not bonded proportionally: wifi=%d cell=%d", snap[0].BytesIn, snap[1].BytesIn)
	}
	if quicWifi == 0 || quicCell == 0 {
		t.Fatalf("SmartAdaptive QUIC 443 did not bond: wifi=%d cell=%d", quicWifi, quicCell)
	}
	if snap[0].Active != 0 || snap[1].Active != 0 {
		t.Fatalf("leaked active slots: %+v", snap)
	}
}

func Test100MB_DataSaver_StrictWifiAndFailover(t *testing.T) {
	needLoopbackAliases(t)
	host, port, stopSrv := startRateLimitedEchoServer(t, "127.0.0.2", "127.0.0.3", 120<<20, 120<<20)
	defer stopSrv()

	s := NewScheduler([]nics.Nic{nic("Mobile data", "127.0.0.3", 1), nic("Wi-Fi", "127.0.0.2", 1)})
	s.SetMode(ModeDataSaver)
	c, w := s.slots[0], s.slots[1]
	s.SetStateWithRTT(w, Fam4, true, 10*time.Millisecond)
	s.SetStateWithRTT(c, Fam4, true, 30*time.Millisecond)
	b := NewBalancer(s)
	stopTick := startTicker(s)
	defer stopTick()

	runDownloadBatch(t, b, host, port, hundredMB, 12)
	snap := s.Snapshot()
	t.Logf("DataSaver 100MB: Wi-Fi=%d MB, Mobile=%d B, active=%d/%d",
		snap[1].BytesIn>>20, snap[0].BytesIn, snap[0].Active, snap[1].Active)
	if snap[0].BytesIn != 0 {
		t.Fatalf("DataSaver leaked %d bytes onto Mobile data while Wi-Fi is healthy", snap[0].BytesIn)
	}
	if snap[1].BytesIn < hundredMB {
		t.Fatalf("DataSaver Wi-Fi bytes=%d, want >= %d", snap[1].BytesIn, hundredMB)
	}

	// Wi-Fi drops -> failover to Mobile data for 10 MB
	s.SetState(w, Fam4, false)
	runDownloadBatch(t, b, host, port, 10<<20, 4)
	snap = s.Snapshot()
	if snap[0].BytesIn < 10<<20 {
		t.Fatalf("DataSaver failover Mobile bytes=%d, want >= %d", snap[0].BytesIn, 10<<20)
	}
	if snap[0].Active != 0 || snap[1].Active != 0 {
		t.Fatalf("leaked active slots: %+v", snap)
	}
}

func Test100MB_SplitEvenly_SymmetricBonding(t *testing.T) {
	needLoopbackAliases(t)
	host, port, stopSrv := startRateLimitedEchoServer(t, "127.0.0.2", "127.0.0.3", 80<<20, 80<<20)
	defer stopSrv()

	s := NewScheduler([]nics.Nic{nic("Wi-Fi", "127.0.0.2", 1), nic("Mobile data", "127.0.0.3", 1)})
	s.SetMode(ModeSplitEvenly)
	w, c := s.slots[0], s.slots[1]
	s.SetStateWithRTT(w, Fam4, true, 10*time.Millisecond)
	s.SetStateWithRTT(c, Fam4, true, 20*time.Millisecond)
	b := NewBalancer(s)
	stopTick := startTicker(s)
	defer stopTick()

	runDownloadBatch(t, b, host, port, hundredMB, 16)
	snap := s.Snapshot()
	t.Logf("SplitEvenly 100MB: Wi-Fi=%d MB (total=%d flows), Mobile=%d MB (total=%d flows)",
		snap[0].BytesIn>>20, snap[0].Total, snap[1].BytesIn>>20, snap[1].Total)
	if snap[0].Total != snap[1].Total {
		t.Fatalf("SplitEvenly flow split not 50/50: wifi=%d cell=%d", snap[0].Total, snap[1].Total)
	}
	if snap[0].BytesIn < 40<<20 || snap[1].BytesIn < 40<<20 {
		t.Fatalf("SplitEvenly byte split skewed: wifi=%d cell=%d", snap[0].BytesIn, snap[1].BytesIn)
	}
	if snap[0].Active != 0 || snap[1].Active != 0 {
		t.Fatalf("leaked active slots: %+v", snap)
	}
}

func TestHandleChangeResetsStateAndClosesStaleConns(t *testing.T) {
	s := NewScheduler([]nics.Nic{{Name: "Wi-Fi", Index: 1, Handle: 100, IP: net.ParseIP("0.0.0.0")}})
	sl := s.slots[0]
	s.SetStateForHandle(sl, 100, Fam4, true, 12*time.Millisecond)
	s.Fail(sl, Fam4, time.Minute)
	closed := 0
	sl.conns.Store(closeCounter{&closed}, struct{}{})

	// Roam to a new networkHandle while IP stays 0.0.0.0
	s.Update([]nics.Nic{{Name: "Wi-Fi", Index: 1, Handle: 200, IP: net.ParseIP("0.0.0.0")}})
	if closed != 1 {
		t.Fatalf("expected 1 stale conn closed on handle change, got %d", closed)
	}
	if sl.state[Fam4] != stateUnknown {
		t.Fatalf("expected stateUnknown after handle change, got %v", sl.state[Fam4])
	}
	if sl.rttMs[Fam4].Load() != 0 {
		t.Fatalf("expected RTT reset on handle change, got %d", sl.rttMs[Fam4].Load())
	}

	// Stale probe from old handle 100 must be ignored
	s.SetStateForHandle(sl, 100, Fam4, false, 0)
	if sl.state[Fam4] != stateUnknown {
		t.Fatalf("stale probe from old handle overwrote state: %v", sl.state[Fam4])
	}
	// Fresh probe from new handle 200 applies normally
	s.SetStateForHandle(sl, 200, Fam4, true, 15*time.Millisecond)
	if sl.state[Fam4] != stateUp || sl.rttMs[Fam4].Load() != 15 {
		t.Fatalf("fresh probe failed: state=%v rtt=%d", sl.state[Fam4], sl.rttMs[Fam4].Load())
	}
}

// Unused sha256 guard to keep crypto/sha256 available if needed
var _ = sha256.Size
