package whatsapp

import (
	"context"

	"go.mau.fi/whatsmeow"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/wire"
)

// Login links Pllum by QR code: the QR channel is opened before connecting,
// each code goes to Pllum as a qr event, and the outcome as a state event.
// A device already linked has nothing to do. A link after an unlink starts
// a new device; its chats continue under the same IDs because they are keyed
// by the people and groups, not the device.
func (w *WhatsApp) Login(ctx context.Context, p network.LoginParams) error {
	if p.Method != network.LoginQR {
		return errUnsupported
	}
	w.mu.Lock()
	if !w.started || w.stopped {
		w.mu.Unlock()
		return errInternal
	}
	if w.state == network.Linking {
		w.mu.Unlock()
		return nil
	}
	// After an unlink whatsmeow deletes the device, perhaps not yet.
	unlinked := w.state == network.ReconnectNeeded
	if w.dev != nil && w.dev.ID != nil && !w.dev.Deleted && !unlinked {
		w.mu.Unlock()
		return nil
	}
	if w.dev == nil || w.dev.Deleted || w.dev.ID != nil {
		w.dev = w.container.NewDevice()
	}
	old := w.cli
	if w.stopCatchUp != nil {
		w.stopCatchUp()
		w.stopCatchUp = nil
	}
	cli := w.makeClient(w.dev)
	lctx, cancel := context.WithCancel(w.ctx)
	qr, err := cli.GetQRChannel(lctx)
	if err != nil {
		cancel()
		w.mu.Unlock()
		return errInternal
	}
	w.loginStop = cancel
	w.loginLost = false
	w.setState(network.Linking, "")
	w.mu.Unlock()
	w.resetHistoryProgress()

	if old != nil {
		old.Disconnect()
	}
	go w.watchLogin(cli, lctx, qr)
	if err := cli.Connect(); err != nil {
		w.endLogin(cli, network.NotConnected, wire.Offline, true)
		return &wire.Error{Kind: wire.Offline}
	}
	return nil
}

// watchLogin turns the QR channel's items into events until the login ends.
func (w *WhatsApp) watchLogin(cli waClient, ctx context.Context, qr <-chan whatsmeow.QRChannelItem) {
	for {
		var item whatsmeow.QRChannelItem
		var ok bool
		select {
		case <-ctx.Done():
			return
		case item, ok = <-qr:
			if !ok {
				return
			}
		}
		switch item.Event {
		case whatsmeow.QRChannelEventCode:
			w.showCode(cli, item)
		case whatsmeow.QRChannelSuccess.Event:
			// whatsmeow reconnects as the linked device; Connected follows.
			// The history sync is done after 120 s even if none comes.
			w.endLogin(cli, network.Connecting, "", false)
			w.armHistoryQuiet()
		case whatsmeow.QRChannelTimeout.Event:
			// The channel says timeout for a dropped connection too.
			kind := wire.Timeout
			w.mu.Lock()
			if w.loginLost {
				kind = wire.Offline
			}
			w.mu.Unlock()
			w.endLogin(cli, network.NotConnected, kind, true)
		case whatsmeow.QRChannelClientOutdated.Event:
			w.endLogin(cli, network.Outdated, "", true)
		case whatsmeow.QRChannelEventPasskeyRequest, whatsmeow.QRChannelEventPasskeyResponse:
			// A passkey link needs a browser's WebAuthn, which Pllum has not.
			w.endLogin(cli, network.NotConnected, wire.Unsupported, true)
		case whatsmeow.QRChannelScannedWithoutMultidevice.Event, whatsmeow.QRChannelEventError:
			w.endLogin(cli, network.NotConnected, wire.Refused, true)
		case whatsmeow.QRChannelErrUnexpectedEvent.Event:
			w.endLogin(cli, network.NotConnected, wire.Internal, true)
		}
	}
}

// showCode sends one QR code to Pllum, with the moment it expires.
func (w *WhatsApp) showCode(cli waClient, item whatsmeow.QRChannelItem) {
	w.mu.Lock()
	current := w.cli == cli && w.loginStop != nil
	w.mu.Unlock()
	if !current {
		return
	}
	w.emit(map[string]any{
		"event":     "qr",
		"code":      item.Code,
		"expiresMs": w.clock.Now().Add(item.Timeout).UnixMilli(),
	})
}

// endLogin ends the QR login cli runs, if it is still the current one, in
// state s; stop also cancels it and disconnects.
func (w *WhatsApp) endLogin(cli waClient, s network.State, kind wire.Kind, stop bool) {
	w.mu.Lock()
	if w.cli != cli || w.loginStop == nil {
		w.mu.Unlock()
		return
	}
	cancel := w.loginStop
	w.loginStop = nil
	if s == network.NotConnected {
		s = w.signedOut()
	}
	w.setState(s, kind)
	w.mu.Unlock()
	if stop {
		cancel()
		cli.Disconnect()
	}
}

// CancelLogin stops a QR login in progress.
func (w *WhatsApp) CancelLogin() {
	w.mu.Lock()
	if w.loginStop == nil {
		w.mu.Unlock()
		return
	}
	cancel, cli := w.loginStop, w.cli
	w.loginStop = nil
	w.setState(w.signedOut(), "")
	w.mu.Unlock()
	cancel()
	cli.Disconnect()
}
