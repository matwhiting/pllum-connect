# Pllum Connect protocol 1

The contract between Pllum and this helper. Pllum refuses any other protocol version.

**Transport.** One JSON object per line, UTF-8, `\n`-terminated, at most 16 MiB a line. Pllum writes requests to the helper's standard input; the helper writes responses and events to its standard output and nothing else. Standard error is never read for content. End of input means "shut down": the helper disconnects and exits within 5 s.

**Shapes.**
- Request: `{"id":"<string>","method":"<name>","params":{…}}`. IDs are Pllum's, unique while the helper runs. Every method but `hello` carries `"network"` in its params.
- Response: `{"id":"…","result":{…}}` or `{"id":"…","error":{"kind":"<kind>"}}`. Responses may come in any order.
- Event: `{"event":"<name>","network":"…",…}`, with no `id`.
- Times are integers in Unix milliseconds, in fields ending `Ms`. Cursors are opaque strings. Unknown fields are ignored on both sides.
- The first request is `hello` `{"protocol":1,"storeKey":"<64 hex>","app":"<Pllum version>"}` → `{"protocol":1,"version":"<helper version>","commit":"<git SHA>","networks":["whatsapp"]}`. Any other version → error `outdated`, and Pllum stops the helper. Before `hello`, every other request is `badRequest`.

**Error kinds** (the only values of `kind`): `badRequest`, `unknownMethod`, `unknownNetwork`, `notConnected` (not signed in), `offline` (signed in, no connection now), `notFound`, `notOnNetwork` (a `beeper:` line), `refused` (WhatsApp turned it down before it was sent; safe to retry), `unclear` (it may have happened), `rateLimited`, `tooLarge`, `mediaGone`, `unsupported`, `storeKey`, `outdated`, `timeout`, `internal`.

**States** (`status` and the `state` event): `notConnected`, `linking`, `connecting`, `catchingUp`, `live`, `offline`, `reconnectNeeded`, `outdated`, `banned`.

**Chat:** `{"id","kind":"single"|"group","title"|null,"phone"|null,"lastActivityMs","lastMessageID"|null,"lastFromMe","unread","muted","readOnly","participants":[{"id","name"|null,"phone"|null,"isSelf"}],"historyFromMs"|null,"historyComplete"}`. `title` is the group's subject, else WhatsApp's name for the person (his phone's contact name, then their push name), else null. `participants` holds at most 64. `muted` is read against the clock: a mute his phone set until a time reads muted until then and unmuted after, and the helper sends `changed` when it lapses.

**Message:** `{"id","chatID","senderID","senderName"|null,"fromMe","timeMs","text"|null,"kind":"text"|"photo"|"video"|"voiceNote"|"sticker"|"file"|"notice","hidden","deleted","edited","replyTo"|null,"media":[{"kind","width"|null,"height"|null,"hasFile"}],"reactions":[{"emoji","senderID","fromMe"}],"seenMs"|null}`.

**Methods:**

| Method | Params (besides `network`) | Result |
|---|---|---|
| `hello` | `protocol`, `storeKey`, `app` | `protocol`, `version`, `commit`, `networks` |
| `shutdown` | — | `{}`, then exits |
| `status` | — | `state`, `selfChatID`\|null |
| `login` | `method` (`"qr"`; `"session"` with `session` is step 3's) | `{}`; progress comes as `qr` and `state` events |
| `cancelLogin`, `logout` | — | `{}` |
| `nudge` | — | `{}`: reconnect now if not connected (after sleep) |
| `chats` | `sinceMs`, `cursor`?, `limit` (≤200) | `items: [Chat]`, `cursor`? (newest activity first) |
| `chat` | `chatID` | `chat: Chat`\|null |
| `messages` | `chatID`, `before`?, `limit` (≤200) | `items: [Message]` oldest first, `hasMore`, `oldestCursor`? |
| `search` | `text`?, `chatIDs`, `fromMe`?, `senderID`?, `afterMs`?, `beforeMs`?, `limit` (≤50), `cursor`? | `items: [Message]` newest first, `cursor`? |
| `findChats` | `query`, `limit` (≤20) | `items: [Chat]` |
| `send` | `chatID`, `text`, `replyTo`?, `file`? (an absolute path), `fileName`?, `clientID`? (the caller's own key for this send) | `messageID` (made before sending; returns at once). A repeat with the `clientID` of a send that is sending or sent returns that send's `messageID` and sends nothing more, even while not connected; one whose send failed makes a new send, which the key then names |
| `sendStatus` | `messageID`, or `clientID` (the key a `send` was made with) instead | `state`: `sending`\|`sent`\|`failed`\|`unknown`, `kind`?; asked by `clientID`, also `messageID` of the send the key names (none, and `unknown`, when it names none). Both or neither is `badRequest` |
| `react` | `chatID`, `messageID`, `emoji` (`""` removes), `transactionID` | `{}` |
| `markRead` | `chatID`, `through`? (a message ID; null = newest incoming) | `{}` |
| `markUnread` | `chatID` | `{}` |
| `setMuted` | `chatID`, `muted` | `{}` |
| `media` | `chatID`, `messageID` | `path` (a file under `connect/media/`) |
| `import` | `beeperChatID`, `chatID`\|null, `chat: {kind, title?, phone?}`, `messages: [ImportMessage]`, `replaces`? | `added`, `matched`, `skipped` |
| `beeperChats` | — | `map: {beeperChatID: chatID}` |
| `lineIDs` | `beeperIDs: [String]` (≤1000) | `map: {beeperID: lineID}` |
| `counts` | — | `chats`, `messages`, `imported`, `matched`, `unplacedChats`, `oldestMs`\|null, `nearMisses` (optional) |

`counts.nearMisses` is a measure, a count only: per network, the Beeper-only lines (`source` beeper) in placed chats that have a direct line on the same side (`fromMe`) within 2000 ms, none of which has the same content as the Beeper line under the matcher's `MatchKey` rules. It reads text to compare and returns a number, never text, IDs or JIDs, and changes nothing about matching. A helper that does not send it is read as 0.

`ImportMessage`: `{"beeperID","networkID"|null,"fromMe","senderID"|null,"senderName"|null,"timeMs","text"|null,"kind","hidden","deleted","replyToBeeperID"|null,"media":[{"kind","width","height"}],"reactions":[{"emoji","senderID","fromMe"}]}`. A null `chatID` makes or extends the unplaced chat `unplaced:<beeperChatID>`; `replaces` names an unplaced chat whose rows are dropped once this placed import has them.

**Events:** `state` `{state, kind?}`; `qr` `{code, expiresMs}`; `history` `{chats, messages, done}` (counts while the link-time sync arrives; `done` after a 100% batch or 120 s with none); `changed` `{}` (something to re-read; at most one per 500 ms per network).

**Go interface each network implements** (`internal/network`):

```go
type Network interface {
	Name() string                                     // "whatsapp"
	Start(ctx context.Context) error                  // connects if signed in; returns at once
	Status() Status                                   // State, SelfChatID
	Login(ctx context.Context, p LoginParams) error
	CancelLogin()
	Logout(ctx context.Context) error
	Nudge()
	Send(ctx context.Context, m Outgoing) (messageID string, err error)
	SendState(messageID string) SendState
	React(ctx context.Context, chatID, messageID, emoji, transactionID string) error
	MarkRead(ctx context.Context, chatID, through string) error
	MarkUnread(ctx context.Context, chatID string) error
	SetMuted(ctx context.Context, chatID string, muted bool) error
	Media(ctx context.Context, chatID, messageID string) (path string, err error)
	Stop()
}
```

Reads (`chats`, `chat`, `messages`, `search`, `findChats`) and the Beeper calls (`import`, `beeperChats`, `lineIDs`, `counts`) are answered by `server` from `store` for every network; a network only writes into the store.

