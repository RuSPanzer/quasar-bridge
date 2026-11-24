package bridge

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/coder/websocket"
	"github.com/skrashevich/quasar-bridge/internal/llm"
	"github.com/skrashevich/quasar-bridge/internal/reply"
	"github.com/skrashevich/quasar-bridge/internal/wire"
	header "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/common/event_header"
	tts "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/events/tts_speak"
	server "github.com/skrashevich/quasar-go-genproto/proto/alice/protos/api/alicekit/protocol/server"
)

type probeTurnKey struct{}

// Some UniProxy versions send a raw Opus BOS instead of TtsSpeak. Only
// bind an unowned stream while one explicit probe is awaiting synthesis.
// Called under serverMu; use the regular correlated envelope/close handling.
func (n *Native) bindProbeRaw(ctx context.Context, k websocket.MessageType, sid uint32) error {
	n.mu.Lock()
	delete(n.probeEnded, sid)
	var owner *nativeRequest
	if n.streams[sid] == nil && n.cloudStreams[sid] == nil && n.blocked[sid] == nil {
		for _, p := range n.requests {
			if !p.probeSeek {
				continue
			}
			if owner != nil {
				n.mu.Unlock()
				return errors.New("ambiguous raw probe synthesis")
			}
			owner = p
		}
	}
	if owner == nil {
		n.mu.Unlock()
		return nil
	}
	if owner.canceled || owner.ctx.Err() != nil {
		n.blocked[sid] = owner
		owner.probeSeek = false
		n.mu.Unlock()
		return nil
	}
	owner.probeRaw = true
	id := owner.ttsID
	n.mu.Unlock()
	m := &server.TServerMessage{Message: &server.TServerMessage_Event{Event: &server.TServerMessage_TEvent{Header: &header.TEventHeader{MessageId: newID(), RefMessageId: id, StreamId: new(int64(sid))}, Event: &server.TServerMessage_TEvent_TtsSpeak{TtsSpeak: &tts.TTtsSpeak{Format: "audio/opus"}}}}}
	b, err := (wire.Frame{Server: m, Prefix: []byte("AAPI")}).Marshal()
	if err != nil {
		return err
	}
	return n.server(ctx, k, b)
}

func probeQuestion(text string) string {
	text = strings.TrimSpace(text)
	if rest, ok := cutModelPrefix(text, "алиса"); ok {
		text = trimModelSeparator(rest)
	}
	if rest, ok := cutModelPrefix(text, "тест потока"); ok {
		return trimModelSeparator(rest)
	}
	return ""
}

func (e *Engine) voiceProbe(text string) bool {
	if os.Getenv("QUASAR_CHATGPT_VOICE_PROBE") != "phrases" {
		return false
	}
	lc, ok := e.LLM.(*llm.Client)
	return ok && lc.API == "chatgpt" && probeQuestion(e.stripModelPrefix(text)) != ""
}

// Start the first phrase immediately. The worker requests subsequent phrases
// as their predecessors drain, without blocking the subscription stream.
func (n *Native) probeContext(p *nativeRequest) (context.Context, func()) {
	pending := make(chan string, 4)
	ctx, cancel := context.WithCancel(p.ctx)
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		for {
			select {
			case <-ctx.Done():
				return
			case s := <-pending:
				if err := n.nextProbePhrase(ctx, p, s); err != nil {
					return
				}
			}
		}
	}()
	// Join the worker before the final reply consumes its spoken prefix.
	stop := func() { cancel(); <-exited }
	var buf string
	done := false
	return llm.WithTextPreview(p.ctx, func(delta string) error {
		if err := p.ctx.Err(); err != nil {
			return err
		}
		if done {
			return nil
		}
		if len(buf)+len(delta) > 4096 {
			done = true
			return nil
		}
		buf += delta
		if p.probePrefix == "" {
			buf = strings.TrimLeftFunc(buf, unicode.IsSpace)
		}
		for {
			end := probeSentenceEnd(buf)
			if end == 0 {
				return nil
			}
			s := buf[:end]
			buf = buf[end:]
			// Only natural-language previews. The normal final path handles
			// links, markup, code and other structured output as a whole.
			if strings.ContainsAny(s, "<>[]`*/:+@_#=|\\") || speechText(s) != s || llm.ValidateAnswer(s) != nil {
				done = true
				return nil
			}
			if p.probePrefix == "" {
				if err := n.startProbe(p, s); err != nil {
					return err
				}
			} else {
				select {
				case pending <- s:
				default:
					// Preserve a contiguous preview prefix; never skip a
					// phrase or hold up the network reader for slow TTS.
					done = true
					return nil
				}
			}
		}
	}), stop
}

func probeSentenceEnd(s string) int {
	for i, r := range s {
		if !strings.ContainsRune(".!?…", r) {
			continue
		}
		end := i + utf8.RuneLen(r)
		if end == len(s) {
			return end
		}
		next, _ := utf8.DecodeRuneInString(s[end:])
		if unicode.IsSpace(next) {
			return end
		}
	}
	return 0
}

func (n *Native) startProbe(p *nativeRequest, s string) error {
	n.mu.Lock()
	if p.canceled || p.ctx.Err() != nil {
		n.mu.Unlock()
		return context.Canceled
	}
	p.probePrefix = s
	p.streaming, p.local = true, true
	p.progressAudioDone = make(chan struct{})
	p.ttsID, p.responseID = newID(), newID()
	p.progressTTS = p.ttsID
	p.listenOnStreamStart = true
	n.tts[p.ttsID] = p
	n.mu.Unlock()
	var err error
	if p.gated {
		err = n.sendCancel(p.ctx, p.id)
	} else {
		err = n.ack(p)
	}
	if err == nil {
		err = n.beginDeferred(p)
	}
	if err != nil {
		p.cancel()
		return err
	}
	return nil
}

func (n *Native) nextProbePhrase(ctx context.Context, p *nativeRequest, s string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.progressAudioDone:
	}
	n.mu.Lock()
	if ctx.Err() != nil || p.canceled {
		n.mu.Unlock()
		return context.Canceled
	}
	p.progressAudioDone = make(chan struct{})
	p.ttsID = newID()
	p.progressTTS = p.ttsID // intermediate source: swallow Close, retain player
	n.tts[p.ttsID] = p
	n.mu.Unlock()
	if err := n.deferredPart(p, s, s, false); err != nil {
		p.cancel()
		return err
	}
	if err := n.synthesize(p, s); err != nil {
		p.cancel()
		return err
	}
	p.probeSuffix.WriteString(s)
	return nil
}

func (n *Native) finishProbe(p *nativeRequest, r Resolution) error {
	tail, ok := strings.CutPrefix(r.Answer, p.probePrefix+p.probeSuffix.String())
	if !ok {
		tail = " Не удалось подтвердить продолжение ответа."
		slog.Info("chatgpt_probe_tail_mismatch", "request", requestTag(p.id))
	}
	if strings.TrimSpace(tail) == "" {
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		case <-p.progressAudioDone:
		}
		if err := n.deferredPart(p, "", "", true); err != nil {
			return err
		}
		if err := n.finishSilentAudio(p); err != nil {
			return err
		}
		reply.Delivered(p.ctx)
		return nil
	}
	// Use plain TTS: raw Opus lacks a close for chains produced by markup.
	speech := speechText(tail)
	return n.finishDeferred(p, tail, speech, speech)
}
