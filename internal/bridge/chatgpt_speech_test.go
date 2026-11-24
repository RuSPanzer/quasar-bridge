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
	"github.com/skrashevich/quasar-bridge/internal/assistant"
	"github.com/skrashevich/quasar-bridge/internal/llm"
	"github.com/skrashevich/quasar-bridge/internal/sound"
	"github.com/skrashevich/quasar-bridge/internal/wire"
	"google.golang.org/protobuf/encoding/protowire"
)

type speechTransport func(*http.Request) (*http.Response, error)

func (f speechTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type voiceTestTools struct {
	calls  int
	silent bool
}

func (t *voiceTestTools) Definitions() []llm.Tool {
	return []llm.Tool{{Type: "function", Function: llm.Function{Name: "lookup_fixture", Parameters: map[string]any{"type": "object"}}}}
}
func (t *voiceTestTools) Call(ctx context.Context, name, args string) (any, error) {
	t.calls++
	if t.silent {
		return "queued", sound.Play(ctx, sound.Request{URL: "https://fixture.invalid/song.mp3"})
	}
	return "fixture-result", nil
}

// The terminal SSE event cannot arrive until real speech bytes (not placeholder
// text or keepalive silence) have reached the station-facing writer.
type speechCase struct {
	name                       string
	raw                        bool
	tail                       string
	backlog                    bool
	tools                      bool
	progress, fallback, silent bool
}

func TestChatGPTVoiceAudioBeforeCompletion(t *testing.T) {
	for _, tc := range []speechCase{
		{name: "envelopes", tail: " И конец"},
		{name: "raw-reused-id-no-tail", raw: true},
		{name: "completed-with-backlog", tail: " И конец", backlog: true},
		{name: "after-assistant-tool", tail: " И конец", tools: true},
		{name: "silent-media-tool", tools: true, silent: true},
		{name: "reuse-progress-player", tail: " И конец", progress: true, raw: true},
		{name: "cloud-refusal", tail: " И конец", fallback: true},
	} {
		t.Run(tc.name, func(t *testing.T) { testChatGPTSpeech(t, tc) })
	}
}
func testChatGPTSpeech(t *testing.T, tc speechCase) {
	raw, tail, backlog, withTools := tc.raw, tc.tail, tc.backlog, tc.tools
	release := make(chan struct{})
	allowDeltas := make(chan struct{})
	startDeltas := sync.OnceFunc(func() { close(allowDeltas) })
	if !tc.progress {
		startDeltas()
	}
	unblock := sync.OnceFunc(func() { close(release) })
	const first = "Первая фраза."
	const second, third = " Вторая фраза!", " Третья фраза?"
	answer := first + second + third + tail
	tools := &voiceTestTools{silent: tc.silent}
	lc := &llm.Client{API: "chatgpt", Model: "fixture", HostedTools: []string{"web_search"}, AccessToken: func(context.Context) (string, error) { return "fixture", nil }}
	lc.HTTP = &http.Client{Transport: speechTransport(func(req *http.Request) (*http.Response, error) {
		var body map[string]any
		if err := json.UnmarshalRead(req.Body, &body); err != nil || body["stream"] != true || body["tools"] == nil {
			t.Errorf("expected subscription stream with configured tools: %v", err)
		}
		if withTools && tools.calls == 0 {
			data := `data: {"type":"response.output_item.added","output_index":0,"item":{"type":"message","role":"assistant","id":"intermediate"}}

data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"intermediate","delta":"Я пока только проверяю."}

data: {"type":"response.completed","response":{"status":"completed","output":[{"type":"function_call","call_id":"lookup-1","name":"lookup_fixture","arguments":"{}"}]}}

`
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(data))}, nil
		}
		r, w := io.Pipe()
		go func() {
			defer w.Close()
			select {
			case <-allowDeltas:
			case <-req.Context().Done():
				return
			}
			io.WriteString(w, "data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"Секрет.\"}\n\n")
			io.WriteString(w, "data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"id\":\"m1\",\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\"}}\n\n")
			io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"m1\",\"output_index\":1,\"content_index\":0,\"delta\":\"Первая фра\"}\n\n")
			io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"m1\",\"output_index\":1,\"content_index\":0,\"delta\":\"за.\"}\n\n")
			io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"m1\",\"output_index\":1,\"content_index\":0,\"delta\":\""+second+third+"\"}\n\n")
			select {
			case <-req.Context().Done():
				return
			case <-release:
			}
			io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"commentary\",\"content\":[{\"type\":\"output_text\",\"text\":\"Промежуточное сообщение.\"}]},{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\",\"text\":\""+answer+"\"}]}]}}\n\n")
		}()
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: r}, nil
	})}
	engine := &Engine{LLM: lc}
	if withTools {
		engine.Assistant = &assistant.Agent{Client: lc, Tools: tools, MaxCalls: 2, MaxRounds: 2}
	}
	cfg := DefaultRoutingConfig()
	cfg.DefaultRoute = "llm"
	if tc.fallback {
		cfg.DefaultRoute = "yandex"
	}
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
	resolved := make(chan struct{}, 1)
	n.OnRoute = func(_, _ string) { resolved <- struct{}{} }
	if tc.progress {
		n.ProgressAfter = time.Millisecond
	}
	defer n.Close()
	defer unblock()
	defer startDeltas()
	if err := n.Client(t.Context(), websocket.MessageBinary, cloudGateInput("story", "Расскажи историю про поезд")); err != nil {
		t.Fatal(err)
	}
	if tc.fallback {
		select {
		case <-resolved:
		case <-time.After(2 * time.Second):
			t.Fatal("no cloud route")
		}
		sendCloudGate(t, n, cloudRefusal("story"))
	}
	if tc.silent {
		unblock()
		timer := time.After(2 * time.Second)
		for {
			select {
			case f := <-up:
				if f.Kind() == "TtsGenerate" {
					t.Fatal("spoke over media action")
				}
			case f := <-down:
				if r := f.Server.GetEvent().GetAliceResponse(); r != nil {
					ds := r.GetResponse().GetDirectives()
					if r.GetVoiceResponse().GetHasVoiceResponse() || len(ds) != 1 || ds[0].GetName() != "audio_play" {
						t.Fatal("media action lost or streaming player opened")
					}
					return
				}
			case <-timer:
				t.Fatal("no media action")
			}
		}
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
	if tc.progress {
		feed(nextTTS("а"), 71, []byte{0xf8, 0xaa, 0xbb, 0xcc})
		startDeltas()
	}
	firstID := nextTTS(first)
	if backlog {
		// Hold the first synth open until the authoritative model response
		// has been read AND the phrase worker joined. SSE must not be blocked
		// by a slow TTS or require a second HTTP request for the remainder.
		unblock()
		select {
		case <-resolved:
		case <-time.After(2 * time.Second):
			t.Fatal("completion waited for slow synthesis")
		}
	}
	feed(firstID, 71, packet)
	earlyPackets := [][]byte{packet}
	if !backlog {
		for i, part := range []string{second, third} {
			marker := []byte{0xf8, 0x12, 0x34, byte(i + 1)}
			feed(nextTTS(part), uint32(73+i), marker)
			earlyPackets = append(earlyPackets, marker)
		}
	}
	var voice, text strings.Builder
	var audio []byte
	open, last := 0, 0
	collect := func(f wire.Frame) {
		if f.Kind() == "opaque" {
			if binary.BigEndian.Uint32(f.Raw) != 71 {
				t.Fatalf("audio on a second player stream: %x", f.Raw[:4])
			}
			audio = append(audio, f.Raw[4:]...)
		}
		if f.Kind() == "TtsSpeak" {
			open++
		}
		if part := f.Server.GetEvent().GetDeferredAliceResponse(); part != nil {
			if part.GetIsLast() {
				last++
			}
			text.WriteString(part.GetBaseResponse().GetText())
			b := part.GetBaseResponse().ProtoReflect().GetUnknown()
			_, _, k := protowire.ConsumeTag(b)
			if k > 0 {
				s, _ := protowire.ConsumeString(b[k:])
				voice.WriteString(s)
			}
		}
	}
	for len(down) > 0 {
		f := <-down
		collect(f)
		if f.Server.GetStreamControl().GetClose() != nil {
			t.Fatal("closed before second part")
		}
	}
	if last != 0 && !backlog {
		t.Fatal("final text before response.completed")
	}
	for _, marker := range earlyPackets {
		if bytes.Count(audio, marker) != 1 {
			t.Fatalf("expected one speech packet %x before response.completed", marker)
		}
	}
	unblock()
	if backlog {
		tail = second + third + tail
	}
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
	if closes != 1 || open != 1 || last != 1 || text.String() != answer || voice.String() != answer {
		t.Fatalf("opens=%d closes=%d last=%d text=%q voice=%q", open, closes, last, text.String(), voice.String())
	}
	var ends int
	for len(audio) > 0 {
		if len(audio) < 27 || string(audio[:4]) != "OggS" {
			t.Fatal("invalid joined Ogg stream")
		}
		if audio[5]&4 != 0 {
			ends++
		}
		n := int(audio[26])
		size := 27 + n
		for _, l := range audio[27:size] {
			size += int(l)
		}
		if ends != 0 && size != len(audio) {
			t.Fatal("audio after EOS")
		}
		audio = audio[size:]
	}
	if ends != 1 {
		t.Fatalf("Ogg EOS pages=%d", ends)
	}
	if withTools && tools.calls != 1 {
		t.Fatalf("tool calls=%d", tools.calls)
	}
}

func TestSpeechCanStartBeforeLongUnpunctuatedAnswerEnds(t *testing.T) {
	s := strings.Repeat("длинное слово ", 25) + "незаконченное"
	cut := sentenceEnd(s)
	if cut <= 0 || cut >= len(s) || s[cut] != ' ' {
		t.Fatalf("no early word boundary: %d", cut)
	}
}

func TestLateRawPhraseAfterCancelIsDropped(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var delivered int
	n := NewNative(t.Context(), nil, func(context.Context, websocket.MessageType, []byte) error { delivered++; return nil }, nil)
	p := &nativeRequest{id: "canceled", ctx: ctx, canceled: true, firstPhrase: "Первая фраза.", phraseSession: true, awaitingRaw: true, ttsID: "first"}
	n.requests[p.id] = p
	b := append(binary.BigEndian.AppendUint32(nil, 41), speechFixture(41)...)
	if err := n.Server(t.Context(), websocket.MessageBinary, b); err != nil || delivered != 0 {
		t.Fatalf("late canceled audio: deliveries=%d err=%v", delivered, err)
	}
}
