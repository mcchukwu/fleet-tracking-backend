// tcpsniff is a dev tool, not part of the production binary. It listens
// on raw TCP (and optionally UDP) ports, accepts whatever a real device
// sends, and records it byte-for-byte: a hex dump to the console and to a
// per-connection log, plus the raw bytes to a per-connection .bin file.
//
// Why this exists: the CW230 tracker has no documented protocol (white
// label hardware, no usable vendor docs), and the T98 dashcam's protocol
// is only inferred from its platform compatibility claims. Writing a
// parser against an assumed spec is guessing; writing it against bytes the
// device actually sent is engineering. Capture first, then parse.
//
// What this tool is NOT: a protocol implementation. It does not
// understand any frame, and it does not send a protocol-correct reply.
// Many devices keep retrying their login frame until they get a valid
// acknowledgement — that is fine here, since the first frames are what we
// need, and a correct reply is something the real adapter will do once we
// know the protocol.
//
// Safety: this accepts arbitrary bytes from whoever can reach the port.
// Run it only while capturing, cap what it records (-max-bytes,
// -max-conns), and keep the capture directory out of version control,
// captures contain real device identifiers and vehicle positions.
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type options struct {
	outDir   string
	idle     time.Duration
	maxBytes int64
	ack      []byte
}

func main() {
	tcpAddrs := flag.String("tcp", ":5001,:5013", "comma-separated TCP listen addresses")
	udpAddrs := flag.String("udp", "", "comma-separated UDP listen addresses (optional)")
	outDir := flag.String("out", "captures", "directory for captures (one .bin and one .log per connection)")
	idle := flag.Duration("idle", 2*time.Minute, "close a TCP connection after this long without data")
	maxBytes := flag.Int64("max-bytes", 1<<20, "stop recording a connection after this many bytes")
	maxConns := flag.Int("max-conns", 50, "maximum simultaneous TCP connections")
	ackHex := flag.String("ack-hex", "", "optional hex bytes written back after every received chunk (observation aid only, not a protocol-correct reply)")
	flag.Parse()

	log := slog.Default()

	ack, err := parseHex(*ackHex)
	if err != nil {
		log.Error("invalid -ack-hex", "error", err)
		os.Exit(2)
	}
	if err := os.MkdirAll(*outDir, 0o750); err != nil {
		log.Error("cannot create output directory", "dir", *outDir, "error", err)
		os.Exit(1)
	}

	opts := options{outDir: *outDir, idle: *idle, maxBytes: *maxBytes, ack: ack}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	var connSeq atomic.Int64
	slots := make(chan struct{}, *maxConns)

	for _, addr := range splitList(*tcpAddrs) {
		ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
		if err != nil {
			log.Error("cannot listen (tcp)", "addr", addr, "error", err)
			os.Exit(1)
		}
		log.Info("listening", "proto", "tcp", "addr", ln.Addr().String())
		wg.Add(1)
		go func() {
			defer wg.Done()
			acceptLoop(ctx, ln, slots, &connSeq, opts, log, &wg)
		}()
	}

	for _, addr := range splitList(*udpAddrs) {
		pc, err := (&net.ListenConfig{}).ListenPacket(ctx, "udp", addr)
		if err != nil {
			log.Error("cannot listen (udp)", "addr", addr, "error", err)
			os.Exit(1)
		}
		log.Info("listening", "proto", "udp", "addr", pc.LocalAddr().String())
		wg.Add(1)
		go func() {
			defer wg.Done()
			udpLoop(ctx, pc, opts, log)
		}()
	}

	<-ctx.Done()
	log.Info("shutting down; captures are in " + opts.outDir)
	wg.Wait()
}

func acceptLoop(
	ctx context.Context, ln net.Listener, slots chan struct{}, seq *atomic.Int64,
	opts options, log *slog.Logger, wg *sync.WaitGroup,
) {
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			log.Warn("accept failed", "error", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		select {
		case slots <- struct{}{}:
		default:
			log.Warn("too many connections, refusing", "remote", conn.RemoteAddr().String())
			_ = conn.Close()
			continue
		}

		id := seq.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			handleConn(ctx, conn, id, opts, log)
		}()
	}
}

func handleConn(ctx context.Context, conn net.Conn, id int64, opts options, log *slog.Logger) {
	defer conn.Close()
	remote := conn.RemoteAddr().String()
	start := time.Now()
	log = log.With("conn", id, "remote", remote)

	rec, err := newRecorder(opts.outDir, fmt.Sprintf("%s_%04d_%s", start.UTC().Format("20060102T150405Z"), id, sanitize(remote)), start)
	if err != nil {
		log.Error("cannot open capture files", "error", err)
		return
	}
	defer rec.close()
	log.Info("connected", "capture", rec.base)

	// AfterFunc rather than a goroutine blocked on ctx.Done(): this runs
	// per connection, and a blocked goroutine per connection would leak
	// until shutdown.
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopClose()

	buf := make([]byte, 4096)
	var total int64
	for {
		_ = conn.SetReadDeadline(time.Now().Add(opts.idle))
		n, err := conn.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			total += int64(n)
			rec.record(chunk)
			log.Info("rx", "bytes", n, "head_hex", hex.EncodeToString(chunk[:min(n, 24)]),
				"head_ascii", printable(chunk[:min(n, 48)]), "hint", hint(chunk))
			if len(opts.ack) > 0 {
				if _, werr := conn.Write(opts.ack); werr != nil {
					log.Warn("ack write failed", "error", werr)
				}
			}
			if total >= opts.maxBytes {
				log.Info("capture limit reached, closing", "total_bytes", total)
				return
			}
		}
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				log.Info("closed by peer", "total_bytes", total, "duration", time.Since(start).Round(time.Millisecond))
			case isTimeout(err):
				log.Info("idle timeout, closing", "total_bytes", total)
			case ctx.Err() != nil:
			default:
				log.Warn("read failed", "error", err, "total_bytes", total)
			}
			return
		}
	}
}

func udpLoop(ctx context.Context, pc net.PacketConn, opts options, log *slog.Logger) {
	go func() {
		<-ctx.Done()
		_ = pc.Close()
	}()

	start := time.Now()
	port := sanitize(pc.LocalAddr().String())
	rec, err := newRecorder(opts.outDir, fmt.Sprintf("%s_udp_%s", start.UTC().Format("20060102T150405Z"), port), start)
	if err != nil {
		log.Error("cannot open capture files", "error", err)
		return
	}
	defer rec.close()

	buf := make([]byte, 64*1024)
	var total int64
	for {
		n, from, err := pc.ReadFrom(buf)
		if n > 0 {
			chunk := buf[:n]
			total += int64(n)
			rec.record(chunk)
			log.Info("rx", "proto", "udp", "from", from.String(), "bytes", n,
				"head_hex", hex.EncodeToString(chunk[:min(n, 24)]),
				"head_ascii", printable(chunk[:min(n, 48)]), "hint", hint(chunk))
			if total >= opts.maxBytes {
				log.Info("capture limit reached, no longer recording udp", "total_bytes", total)
				return
			}
		}
		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
				log.Warn("udp read failed", "error", err)
			}
			return
		}
	}
}

// recorder writes each received chunk twice: raw to a .bin (what a parser
// test will replay later) and as a timestamped hex dump to a .log (what a
// human reads to work out the framing).
type recorder struct {
	mu    sync.Mutex
	base  string
	start time.Time
	bin   *os.File
	txt   *os.File
}

func newRecorder(dir, base string, start time.Time) (*recorder, error) {
	// 0o640: captures contain real device ids and vehicle positions.
	bin, err := os.OpenFile(filepath.Join(dir, base+".bin"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return nil, err
	}
	txt, err := os.OpenFile(filepath.Join(dir, base+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		_ = bin.Close()
		return nil, err
	}
	return &recorder{base: base, start: start, bin: bin, txt: txt}, nil
}

func (r *recorder) record(chunk []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, _ = r.bin.Write(chunk)
	fmt.Fprintf(r.txt, "[%s +%s] %d bytes\n%s\n",
		time.Now().UTC().Format(time.RFC3339Nano), time.Since(r.start).Round(time.Millisecond),
		len(chunk), hex.Dump(chunk))
}

func (r *recorder) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	_ = r.bin.Close()
	_ = r.txt.Close()
}

// hint guesses a protocol family from the first bytes. It is a pointer
// for where to look first, never a verdict, confirm against the full
// capture and the vendor's own documentation before writing a parser.
func hint(b []byte) string {
	switch {
	case len(b) >= 1 && b[0] == 0x7e:
		return "starts with 0x7E: possible JT/T808 frame delimiter"
	case len(b) >= 2 && b[0] == 0x78 && b[1] == 0x78:
		return "starts with 0x7878: possible GT06/Concox"
	case len(b) >= 2 && b[0] == 0x79 && b[1] == 0x79:
		return "starts with 0x7979: possible GT06/Concox extended frame"
	case len(b) >= 17 && b[0] == 0x00 && b[1] == 0x0f && allDigits(b[2:17]):
		return "0x000F then 15 ASCII digits: possible Teltonika IMEI handshake"
	case strings.HasPrefix(string(b[:min(len(b), 5)]), "imei:"):
		return "starts with \"imei:\": possible GPS103-style plaintext"
	case strings.HasPrefix(string(b[:min(len(b), 3)]), "*HQ"):
		return "starts with \"*HQ\": possible H02-style plaintext"
	default:
		return "no known signature; read the full hex dump"
	}
}

func allDigits(b []byte) bool {
	for _, c := range b {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func printable(b []byte) string {
	out := make([]byte, len(b))
	for i, c := range b {
		if c >= 0x20 && c < 0x7f {
			out[i] = c
		} else {
			out[i] = '.'
		}
	}
	return string(out)
}

func sanitize(s string) string {
	return strings.NewReplacer(":", "_", "[", "", "]", "", "/", "_", "%", "_").Replace(s)
}

func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func parseHex(s string) ([]byte, error) {
	s = strings.ReplaceAll(strings.TrimSpace(s), " ", "")
	if s == "" {
		return nil, nil
	}
	return hex.DecodeString(s)
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
