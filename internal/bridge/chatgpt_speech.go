package bridge

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
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

// Some UniProxy versions send a raw Opus BOS instead of TtsSpeak. Only
// bind an unowned stream while one phrase response is awaiting synthesis.
// Called under serverMu; use the regular correlated envelope/close handling.
func (n *Native) bindRawPhrase(ctx context.Context, k websocket.MessageType, sid uint32) error {
	n.mu.Lock()
	delete(n.endedRaw, sid)
	var owner *nativeRequest
	if n.streams[sid] == nil && n.cloudStreams[sid] == nil && n.blocked[sid] == nil {
		for _, p := range n.requests {
			if !p.awaitingRaw {
				continue
			}
			if owner != nil {
				n.mu.Unlock()
				return errors.New("ambiguous raw phrase synthesis")
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
		owner.awaitingRaw = false
		n.mu.Unlock()
		return nil
	}
	owner.rawPhrase = true
	id := owner.ttsID
	n.mu.Unlock()
	m := &server.TServerMessage{Message: &server.TServerMessage_Event{Event: &server.TServerMessage_TEvent{Header: &header.TEventHeader{MessageId: newID(), RefMessageId: id, StreamId: new(int64(sid))}, Event: &server.TServerMessage_TEvent_TtsSpeak{TtsSpeak: &tts.TTtsSpeak{Format: "audio/opus"}}}}}
	b, err := (wire.Frame{Server: m, Prefix: []byte("AAPI")}).Marshal()
	if err != nil {
		return err
	}
	return n.server(ctx, k, b)
}

func (n *Native) streamsChatGPT() bool {
	e, ok := n.resolver.(*Engine)
	if !ok {
		return false
	}
	lc, ok := e.LLM.(*llm.Client)
	return ok && lc.API == "chatgpt"
}

// Start the first phrase immediately. The worker requests subsequent phrases
// as their predecessors drain, without blocking the subscription stream.
func (n *Native) phraseContext(p *nativeRequest) (context.Context, func()) {
	n.mu.Lock()
	p.phraseSession = true
	n.mu.Unlock()
	stopProgress := func() {}
	if !p.cloud && n.ProgressAfter > 0 {
		stopProgress = sync.OnceFunc(n.progress(p, true))
	}
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
				if err := n.nextPhrase(ctx, p, s); err != nil {
					return
				}
			}
		}
	}()
	// Join the worker before the final reply consumes its spoken prefix.
	stop := func() { stopProgress(); cancel(); <-exited }
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
		if p.firstPhrase == "" {
			buf = strings.TrimLeftFunc(buf, unicode.IsSpace)
		}
		for {
			end := sentenceEnd(buf)
			if end == 0 {
				return nil
			}
			s := buf[:end]
			buf = buf[end:]
			// Only natural-language previews. The normal final path handles
			// links, markup, code and other structured output as a whole.
			if strings.ContainsAny(s, "<>[]`*/+@_#=|\\") || speechText(s) != s || llm.ValidateAnswer(s) != nil {
				done = true
				return nil
			}
			if p.firstPhrase == "" {
				if n.hasSilentPlayback(p) {
					done = true
					return nil
				}
				stopProgress()
				if err := n.startPhrases(p, s); err != nil {
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

func sentenceEnd(s string) int {
	// A long answer need not contain punctuation. Keep the separator
	// with the next phrase so the prefix still matches the final text.
	const maxRunes = 160
	var runes, boundary int
	var inWord bool
	for i, r := range s {
		runes++
		if unicode.IsSpace(r) && inWord {
			boundary = i
		}
		inWord = !unicode.IsSpace(r)
		if runes >= maxRunes && boundary > 0 {
			return boundary
		}
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

func (n *Native) startPhrases(p *nativeRequest, s string) error {
	n.mu.Lock()
	if p.canceled || p.ctx.Err() != nil {
		n.mu.Unlock()
		return context.Canceled
	}
	if p.streaming {
		// A slow model/tool turn already opened the progress player. Reuse
		// it after the silent header source drains instead of opening again.
		n.mu.Unlock()
		return n.nextPhrase(p.ctx, p, s)
	}
	p.firstPhrase = s
	p.streaming, p.local = true, true
	p.progressAudioDone = make(chan struct{})
	p.ttsID, p.responseID = newID(), newID()
	p.progressTTS = p.ttsID
	n.tts[p.ttsID] = p
	n.mu.Unlock()
	var err error
	if p.gated || p.cloud {
		err = n.sendCancel(p.ctx, p.id)
	} else {
		err = n.ack(p)
	}
	if err == nil {
		err = n.beginDeferred(p, true)
	}
	if err != nil {
		p.cancel()
		return err
	}
	return nil
}

func (n *Native) nextPhrase(ctx context.Context, p *nativeRequest, s string) error {
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
	first := p.firstPhrase == ""
	if first {
		p.firstPhrase = s
	}
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
	if !first {
		p.laterPhrases.WriteString(s)
	}
	return nil
}

func (n *Native) finishPhrases(p *nativeRequest, r Resolution) error {
	select {
	case <-p.ctx.Done():
		return p.ctx.Err()
	case <-p.progressAudioDone:
	}
	spoken := p.firstPhrase + p.laterPhrases.String()
	tail, ok := strings.CutPrefix(r.Answer, spoken)
	if !ok {
		tail = " Не удалось подтвердить продолжение ответа."
		slog.Info("chatgpt_speech_tail_mismatch", "request", requestTag(p.id))
	}
	// Even without a spoken tail, a queued media action needs finishDeferred's
	// ordering: close Dialog audio before handing playback to the media player.
	if strings.TrimSpace(tail) == "" && !n.hasSilentPlayback(p) {
		if err := n.revokeDeferredListenIfNeeded(p); err != nil {
			return err
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
	voice := speechText(tail)
	speech := voice
	// Keep model-requested TTS markup on correlated streams. Raw Opus
	// lacks a close identifying the end of a chain produced by markup.
	n.mu.Lock()
	raw := p.rawPhrase
	n.mu.Unlock()
	if ok && r.speech != "" && !raw {
		if rest, match := strings.CutPrefix(r.speech, spoken); match {
			speech = speechText(rest)
		}
	}
	return n.finishDeferred(p, tail, voice, speech)
}
