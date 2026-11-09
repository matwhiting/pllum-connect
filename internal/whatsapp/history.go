package whatsapp

import (
	"context"
	"errors"
	"io"
	"net"
	"sort"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/store"
)

// History arrives as notifications, each naming a blob his phone uploaded.
// A notification is kept in pending_history before WhatsApp is told it
// arrived (ingest), and this worker downloads the blobs oldest first, stores
// every conversation in them, and only then deletes the row. A row left by
// a run that stopped is processed at the next start; storing a line twice
// is harmless, as lines are keyed by their IDs.

// kickHistory wakes the worker.
func (w *WhatsApp) kickHistory() {
	select {
	case w.kick <- struct{}{}:
	default:
	}
}

func (w *WhatsApp) historyLoop(ctx context.Context) {
	defer w.worker.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.kick:
		}
		w.drainHistory(ctx)
	}
}

// drainHistory processes every waiting notification it can.
func (w *WhatsApp) drainHistory(ctx context.Context) {
	for ctx.Err() == nil {
		id, blob, ok, err := w.st.NextPendingHistory(Name)
		if err != nil || !ok {
			return
		}
		var notif waE2E.HistorySyncNotification
		if err := proto.Unmarshal(blob, &notif); err != nil {
			if w.dropPending(id) != nil {
				return
			}
			continue
		}
		// A download needs the connection; Connected wakes the worker.
		cli := w.client()
		if cli == nil || !w.online() {
			return
		}
		hs, err := cli.DownloadHistorySync(ctx, &notif, true)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			gone := historyGone(err)
			// Only a failure while connected that is not the connection's own
			// counts towards giving the notification up.
			if !gone && !historyTransient(err) && w.online() && w.failed(id) >= historyTries {
				gone = true
			}
			if gone {
				if w.dropPending(id) != nil {
					return
				}
				continue
			}
			w.retryHistory()
			return
		}
		complete, err := w.storeHistory(cli, hs)
		if err != nil {
			w.retryHistory()
			return
		}
		if w.dropPending(id) != nil {
			return
		}
		// Told only once the row is gone, so the done timer never finds it
		// still waiting.
		w.historyProgress(complete)
		w.changed()
	}
}

// dropPending deletes a pending row and forgets its failures: ids are
// reused once the table empties, so a later row must not inherit them.
func (w *WhatsApp) dropPending(id int64) error {
	if err := w.st.DeletePendingHistory(Name, id); err != nil {
		return err
	}
	w.mu.Lock()
	delete(w.tries, id)
	w.mu.Unlock()
	return nil
}

// historyGone reports whether a download failed for good: the blob is gone
// from WhatsApp's servers or is not what the notification promised.
func historyGone(err error) bool {
	for _, gone := range []error{
		whatsmeow.ErrMediaDownloadFailedWith404, whatsmeow.ErrMediaDownloadFailedWith410,
		whatsmeow.ErrNoURLPresent, whatsmeow.ErrFileLengthMismatch, whatsmeow.ErrInvalidMediaSHA256,
		whatsmeow.ErrInvalidMediaEncSHA256, whatsmeow.ErrInvalidMediaHMAC,
	} {
		if errors.Is(err, gone) {
			return true
		}
	}
	return false
}

// historyTransient reports whether a download failed for want of a working
// connection: not connected, an info query cut off or timed out, a network
// error, a cancelled context, or the media server busy. These never count
// towards giving a notification up.
func historyTransient(err error) bool {
	if errors.Is(err, whatsmeow.ErrNotConnected) || errors.Is(err, whatsmeow.ErrIQTimedOut) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var disc *whatsmeow.DisconnectedError
	if errors.As(err, &disc) {
		return true
	}
	var nerr net.Error
	if errors.As(err, &nerr) {
		return true
	}
	var herr whatsmeow.DownloadHTTPError
	if errors.As(err, &herr) && herr.Response != nil && (herr.StatusCode >= 500 || herr.StatusCode == 429) {
		return true
	}
	return false
}

// online reports whether the connection is up: catching up or live.
func (w *WhatsApp) online() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.state == network.CatchingUp || w.state == network.Live
}

// failed counts one more failed download of row id in this run.
func (w *WhatsApp) failed(id int64) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tries[id]++
	return w.tries[id]
}

// retryHistory wakes the worker again in a while, once.
func (w *WhatsApp) retryHistory() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.retrying || w.stopped {
		return
	}
	w.retrying = true
	w.clock.AfterFunc(historyRetry, func() {
		w.mu.Lock()
		w.retrying = false
		w.mu.Unlock()
		w.kickHistory()
	})
}

// storeHistory writes one downloaded history blob: names, number–LID
// mappings, and each conversation with its lines, unread state and mute.
// complete reports a FULL sync at 100%.
func (w *WhatsApp) storeHistory(cli waClient, hs *waHistorySync.HistorySync) (complete bool, err error) {
	for _, m := range hs.GetPhoneNumberToLidMappings() {
		pn, err1 := types.ParseJID(m.GetPnJID())
		lid, err2 := types.ParseJID(m.GetLidJID())
		if err1 == nil && err2 == nil {
			if err := w.link(lid, pn); err != nil {
				return false, err
			}
		}
	}
	if hs.GetSyncType() == waHistorySync.HistorySync_PUSH_NAME {
		var jids []types.JID
		for _, p := range hs.GetPushnames() {
			if j, err := types.ParseJID(p.GetID()); err == nil {
				jids = append(jids, j)
			}
		}
		if err := w.refreshTitles(jids...); err != nil {
			return false, err
		}
	}

	full := hs.GetSyncType() == waHistorySync.HistorySync_FULL
	var chats []string
	messages := 0
	for _, conv := range hs.GetConversations() {
		chatID, n, err := w.storeConversation(cli, conv)
		if err != nil {
			return false, err
		}
		if chatID != "" {
			chats = append(chats, chatID)
			messages += n
		}
	}

	w.hmu.Lock()
	for _, c := range chats {
		w.hChats[c] = true
		if full {
			w.hFull[c] = true
		}
	}
	w.hMessages += messages
	complete = full && hs.GetProgress() >= 100
	var completed []string
	if complete {
		for c := range w.hFull {
			completed = append(completed, c)
		}
	}
	w.hmu.Unlock()

	if complete {
		sort.Strings(completed)
		yes := true
		for _, c := range completed {
			if err := w.st.PutChat(Name, store.ChatInfo{ID: c, HistoryComplete: &yes}); err != nil {
				return false, err
			}
		}
	}
	return complete, nil
}

// storeConversation writes one conversation; chatID is empty for a chat
// that is not kept (Status, a Channel).
func (w *WhatsApp) storeConversation(cli waClient, conv *waHistorySync.Conversation) (chatID string, n int, err error) {
	jid, err := types.ParseJID(conv.GetID())
	if err != nil || !allowedChat(jid) {
		return "", 0, nil
	}
	jid = norm(jid)
	if jid.Server == types.HiddenUserServer && conv.GetPnJID() != "" {
		if pn, err := types.ParseJID(conv.GetPnJID()); err == nil && norm(pn).Server == types.DefaultUserServer {
			if dev := w.device(); dev != nil && dev.LIDs != nil {
				_ = dev.LIDs.PutLIDMapping(context.WithoutCancel(w.bg()), jid, norm(pn))
			}
		}
	}
	if chatID, err = w.chatID(jid); err != nil {
		return "", 0, err
	}
	if err := w.st.PutChat(Name, w.chatInfo(chatID, jid, conv)); err != nil {
		return "", 0, err
	}

	// Every line is worked out first, so the read marker moves before the
	// lines land and none of them shows unread in between.
	var preps []prep
	var incoming []int64
	var newest int64
	for _, hm := range conv.GetMessages() {
		evt, err := cli.ParseWebMessage(jid, hm.GetMessage())
		if err != nil {
			continue
		}
		p, err := w.prepare(evt, store.SourceSync)
		if err != nil {
			return "", 0, err
		}
		preps = append(preps, p)
		if p.line {
			n++
			newest = max(newest, p.puts[0].TimeMs)
		}
		if p.incoming > 0 {
			incoming = append(incoming, p.incoming)
		}
	}
	if through, ok := readThrough(conv, incoming, newest); ok {
		if err := w.st.PutChat(Name, store.ChatInfo{ID: chatID, ReadThroughMs: &through, ReadForwardOnly: true}); err != nil {
			return "", 0, err
		}
	}
	for _, p := range preps {
		if err := w.apply(p); err != nil {
			return "", 0, err
		}
	}
	return chatID, n, nil
}

// chatInfo is what a history conversation says about its chat.
func (w *WhatsApp) chatInfo(chatID string, jid types.JID, conv *waHistorySync.Conversation) store.ChatInfo {
	info := store.ChatInfo{ID: chatID, Kind: "single"}
	name := conv.GetName()
	if name == "" {
		name = conv.GetDisplayName()
	}
	if jid.Server == types.GroupServer {
		info.Kind = "group"
		for _, p := range conv.GetParticipant() {
			pj, err := types.ParseJID(p.GetUserJID())
			if err != nil || pj.User == "" {
				continue
			}
			info.Participants = append(info.Participants, store.Participant{
				ID: w.personID(pj), Name: w.titleOf(pj), Phone: w.phoneOf(pj), IsSelf: w.isSelf(pj),
			})
		}
	} else {
		if t := w.titleOf(jid); t != "" {
			name = t
		}
		if p := w.phoneOf(jid); p != "" {
			info.Phone = &p
		}
	}
	if name != "" {
		info.Title = &name
	}
	if conv.MuteEndTime != nil {
		end := int64(conv.GetMuteEndTime())
		if conv.GetMuteEndTime() > 1<<62 {
			end = -1 // for ever
		}
		muted := end != 0 && mutedUntil(true, end, w.clock.Now().UnixMilli())
		info.Muted = &muted
	}
	return info
}

// readThrough turns WhatsApp's unread count for a conversation into his read
// marker: everything up to the newest line when nothing is unread, else up
// to just before the oldest of the unread incoming lines. A batch that does
// not reach the conversation's newest line says nothing about which lines
// are unread unless nothing is.
func readThrough(conv *waHistorySync.Conversation, incoming []int64, newest int64) (int64, bool) {
	last := msOf(int64(conv.GetLastMsgTimestamp()))
	if last == 0 {
		last = msOf(int64(conv.GetConversationTimestamp()))
	}
	unread := int(conv.GetUnreadCount())
	if unread == 0 && !conv.GetMarkedAsUnread() {
		through := max(newest, last)
		return through, through > 0
	}
	if newest == 0 || newest < last {
		return 0, false
	}
	sort.Slice(incoming, func(i, j int) bool { return incoming[i] > incoming[j] })
	if unread == 0 { // marked unread: the newest incoming line shows unread
		unread = 1
	}
	if len(incoming) == 0 {
		return newest, true
	}
	if len(incoming) > unread {
		return incoming[unread], true
	}
	return incoming[len(incoming)-1] - 1, true
}

// historyProgress tells Pllum how far the history sync has come. It is done
// when a full sync reaches 100%, or 120 s after the last batch.
func (w *WhatsApp) historyProgress(complete bool) {
	w.hmu.Lock()
	if complete {
		w.hDone = true
	}
	if w.hStop != nil {
		w.hStop()
		w.hStop = nil
	}
	if !w.hDone {
		w.hStop = w.clock.AfterFunc(historyQuiet, w.historyQuietEnded)
	}
	ev := w.historyEvent()
	w.hmu.Unlock()
	w.emit(ev)
}

// armHistoryQuiet starts the 120 s wait at a new link, so a phone that
// sends no history still ends in a done history event.
func (w *WhatsApp) armHistoryQuiet() {
	w.hmu.Lock()
	defer w.hmu.Unlock()
	if w.hStop != nil {
		w.hStop()
		w.hStop = nil
	}
	if !w.hDone {
		w.hStop = w.clock.AfterFunc(historyQuiet, w.historyQuietEnded)
	}
}

// holdHistoryQuiet restarts the 120 s wait when a notification arrives,
// unless the sync is already done.
func (w *WhatsApp) holdHistoryQuiet() {
	w.hmu.Lock()
	armed := w.hStop != nil
	w.hmu.Unlock()
	if armed {
		w.armHistoryQuiet()
	}
}

// historyQuietEnded: 120 s with no batch, so the history sync is done,
// unless a notification is still waiting to be downloaded (for a connection,
// say); then the wait starts again.
func (w *WhatsApp) historyQuietEnded() {
	w.mu.Lock()
	stopped := w.stopped
	w.mu.Unlock()
	if stopped {
		return
	}
	if _, _, waiting, err := w.st.NextPendingHistory(Name); waiting || err != nil {
		w.armHistoryQuiet()
		return
	}
	w.hmu.Lock()
	if w.hDone {
		w.hmu.Unlock()
		return
	}
	w.hDone = true
	ev := w.historyEvent()
	w.hmu.Unlock()
	w.emit(ev)
}

// resetHistoryProgress starts the counts again for a new link's sync.
func (w *WhatsApp) resetHistoryProgress() {
	w.hmu.Lock()
	defer w.hmu.Unlock()
	if w.hStop != nil {
		w.hStop()
		w.hStop = nil
	}
	w.hChats, w.hFull, w.hMessages, w.hDone = map[string]bool{}, map[string]bool{}, 0, false
}

// historyEvent is the history event as things stand; w.hmu is held.
func (w *WhatsApp) historyEvent() map[string]any {
	return map[string]any{"event": "history", "chats": len(w.hChats), "messages": w.hMessages, "done": w.hDone}
}
