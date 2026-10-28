// Command wsprobe verifies that the authenticated live feed delivers at
// least one message. It is used by the integration script and during
// frontend debugging.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/coder/websocket"
)

func main() {
	apiURL := flag.String("api-url", "http://localhost:8081", "console API base URL")
	email := flag.String("email", "dispatcher@demo.ftb", "login email")
	password := flag.String("password", "DemoPass123!", "login password")
	timeout := flag.Duration("timeout", 10*time.Second, "time to wait for one message")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}
	body, _ := json.Marshal(map[string]string{"email": *email, "password": *password})
	resp, err := client.Post(*apiURL+"/api/v1/auth/login", "application/json", bytes.NewReader(body))
	if err != nil {
		fail("login: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fail("login: status %d", resp.StatusCode)
	}

	httpBase, err := url.Parse(*apiURL)
	if err != nil {
		fail("parse api url: %v", err)
	}
	cookies := client.Jar.Cookies(httpBase)

	base := *httpBase
	base.Scheme = strings.Replace(base.Scheme, "http", "ws", 1)
	base.Path = "/api/v1/live/ws"
	parts := make([]string, 0, len(cookies))
	for _, c := range cookies {
		parts = append(parts, c.Name+"="+c.Value)
	}

	conn, _, err := websocket.Dial(ctx, base.String(), &websocket.DialOptions{
		HTTPHeader: map[string][]string{"Cookie": {strings.Join(parts, "; ")}},
	})
	if err != nil {
		fail("dial: %v", err)
	}
	defer conn.CloseNow()

	_, data, err := conn.Read(ctx)
	if err != nil {
		fail("read: %v", err)
	}
	var msg struct {
		Type     string `json:"type"`
		TenantID string `json:"tenant_id"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		fail("decode: %v", err)
	}
	fmt.Printf("ok type=%s tenant=%s\n", msg.Type, msg.TenantID)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
