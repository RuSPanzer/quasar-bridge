package bridge

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json/v2"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/skrashevich/quasar-bridge/internal/llm"
	"github.com/skrashevich/quasar-bridge/internal/wire"
	common "github.com/skrashevich/quasar-go-genproto/proto/alice/megamind/protos/common"
	request "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/alice_request"
	header "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/common/event_header"
	client "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/client"
	"google.golang.org/protobuf/encoding/protowire"
)

type probeTransport func(*http.Request) (*http.Response, error)

func (f probeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// The terminal SSE event cannot arrive until real speech bytes (not placeholder
// text or keepalive silence) have reached the station-facing writer.
func TestChatGPTProbeAudioBeforeCompletion(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  bool
		tail string
	}{
		{"envelopes", false, " Вторая фраза."},
		{"raw-reused-id", true, " Вторая фраза."},
		{"raw-no-tail", true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) { testChatGPTProbe(t, tc.raw, tc.tail) })
	}
}
func testChatGPTProbe(t *testing.T, raw bool, tail string) {
	t.Setenv("QUASAR_CHATGPT_VOICE_PROBE", "early")
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	const first = "Первая фраза."
	lc := &llm.Client{API: "chatgpt", Model: "fixture", HostedTools: []string{"web_search"}, AccessToken: func(context.Context) (string, error) { return "fixture", nil }}
	lc.HTTP = &http.Client{Transport: probeTransport(func(req *http.Request) (*http.Response, error) {
		var body map[string]any
		if err := json.UnmarshalRead(req.Body, &body); err != nil || body["stream"] != true || body["tools"] != nil {
			t.Errorf("expected one tool-free subscription stream: %v", err)
		}
		r, w := io.Pipe()
		go func() {
			defer w.Close()
			io.WriteString(w, "data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"Секрет.\"}\n\n")
			io.WriteString(w, "data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"id\":\"m1\",\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\"}}\n\n")
			io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"m1\",\"output_index\":1,\"content_index\":0,\"delta\":\"Первая фра\"}\n\n")
			io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"m1\",\"output_index\":1,\"content_index\":0,\"delta\":\"за.\"}\n\n")
			select {
			case <-req.Context().Done():
				return
			case <-release:
			}
			io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\""+first+tail+"\"}]}]}}\n\n")
		}()
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: r}, nil
	})}
	engine := &Engine{LLM: lc}
	cfg := DefaultRoutingConfig()
	cfg.DefaultRoute = "llm"
	if err := engine.SetRouting(cfg); err != nil {
		t.Fatal(err)
	}
	up, down := make(chan wire.Frame, 32), make(chan wire.Frame, 64)
	n := NewNative(t.Context(), func(_ context.Context, _ websocket.MessageType, b []byte) error {
		up <- wire.Decode(b, wire.ClientToServer)
		return nil
	}, func(_ context.Context, _ websocket.MessageType, b []byte) error {
		down <- wire.Decode(b, wire.ServerToClient)
		return nil
	}, engine)
	defer n.Close()
	defer unblock()
	input := &client.TClientMessage{Message: &client.TClientMessage_Event{Event: &client.TClientMessage_TEvent{Header: &header.TEventHeader{MessageId: "probe"}, Event: &client.TClientMessage_TEvent_VoiceInput{VoiceInput: &request.TAliceRequestProto{Request: &request.TAliceRequestProto_TRequest{Event: &common.TEvent{Text: new("тест потока расскажи историю")}}}}}}}
	if err := n.Client(t.Context(), websocket.MessageBinary, aa(input)); err != nil {
		t.Fatal(err)
	}
	nextTTS := func(want string) string {
		t.Helper()
		timer := time.After(2 * time.Second)
		for {
			select {
			case f := <-up:
				if g := f.Client.GetEvent().GetTtsGenerate(); g != nil {
					if strings.TrimSpace(g.GetText()) != strings.TrimSpace(want) {
						t.Fatalf("TTS = %q, want %q", g.GetText(), want)
					}
					return f.Client.GetEvent().GetHeader().GetMessageId()
				}
			case <-timer:
				t.Fatalf("no synthesis for %q", want)
			}
		}
	}
	packet := []byte{0xf8, 0x12, 0x34, 0x56}
	feed := func(id string, sid uint32, packet []byte) {
		if !raw {
			feedSpeech(t, n, id, sid, false, packet)
			return
		}
		audio := append(binary.BigEndian.AppendUint32(nil, 71), speechPacketFixture(sid, packet)...)
		if err := n.Server(t.Context(), websocket.MessageBinary, audio); err != nil {
			t.Fatal(err)
		}
	}
	feed(nextTTS(first), 71, packet)
	var voice, text strings.Builder
	collect := func(f wire.Frame) {
		if part := f.Server.GetEvent().GetDeferredAliceResponse(); part != nil {
			text.WriteString(part.GetBaseResponse().GetText())
			b := part.GetBaseResponse().ProtoReflect().GetUnknown()
			_, _, k := protowire.ConsumeTag(b)
			if k > 0 {
				s, _ := protowire.ConsumeString(b[k:])
				voice.WriteString(s)
			}
		}
	}
	earlyAudio := false
	for len(down) > 0 {
		f := <-down
		collect(f)
		if f.Kind() == "opaque" && bytes.Contains(f.Raw, packet) {
			earlyAudio = true
		}
		if f.Server.GetStreamControl().GetClose() != nil {
			t.Fatal("closed before second part")
		}
	}
	if !earlyAudio {
		t.Fatal("no first speech payload before response.completed")
	}
	unblock()
	if tail != "" {
		feed(nextTTS(tail), 72, []byte{0xf8, 0x56, 0x34, 0x12})
	}
	closes := 0
	deadline := time.After(2 * time.Second)
	for closes == 0 || len(down) > 0 {
		select {
		case f := <-down:
			collect(f)
			if f.Server.GetStreamControl().GetClose() != nil {
				closes++
			}
		case <-deadline:
			t.Fatal("joined player did not close")
		}
	}
	if closes != 1 || text.String() != first+tail || voice.String() != first+tail {
		t.Fatalf("closes=%d text=%q voice=%q", closes, text.String(), voice.String())
	}
}

func TestProbeLateRawSpeechAfterCancelIsDropped(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var delivered int
	n := NewNative(t.Context(), nil, func(context.Context, websocket.MessageType, []byte) error { delivered++; return nil }, nil)
	p := &nativeRequest{id: "canceled", ctx: ctx, canceled: true, probePrefix: "Первая фраза.", probeSeek: true, ttsID: "first"}
	n.requests[p.id] = p
	b := append(binary.BigEndian.AppendUint32(nil, 41), speechFixture(41)...)
	if err := n.Server(t.Context(), websocket.MessageBinary, b); err != nil || delivered != 0 {
		t.Fatalf("late canceled audio: deliveries=%d err=%v", delivered, err)
	}
}

func TestProbeActivationIsExplicit(t *testing.T) {
	for _, tc := range []struct {
		mode, api, query string
		want             bool
	}{
		{"early", "chatgpt", "Алиса, тест потока, расскажи сказку", true},
		{"baseline", "chatgpt", "Тест потока: расскажи сказку", true},
		{"early", "chatgpt", "расскажи сказку", false},
		{"", "chatgpt", "тест потока расскажи сказку", false},
		{"early", "responses", "тест потока расскажи сказку", false},
		{"early", "chatgpt", "тест потоками", false},
	} {
		t.Setenv("QUASAR_CHATGPT_VOICE_PROBE", tc.mode)
		e := Engine{LLM: &llm.Client{API: tc.api}}
		if got := e.voiceProbe(tc.query); got != tc.want {
			t.Fatalf("mode=%s api=%s query=%q: %v", tc.mode, tc.api, tc.query, got)
		}
	}
}
