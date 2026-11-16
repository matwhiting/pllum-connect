# Pllum Connect protocol 1

The contract between Pllum and this helper. Pllum refuses any other protocol version.

**Transport.** One JSON object per line, UTF-8, `\n`-terminated, at most 16 MiB a line. Pllum writes requests to the helper's standard input; the helper writes responses and events to its standard output and nothing else. Standard error is never read for content. End of input means "shut down": the helper disconnects and exits within 5 s.

**Shapes.**
- Request: `{"id":"<string>","method":"<name>","params":{…}}`. IDs are Pllum's, unique while the helper runs. Every method but `hello` carries `"network"` in its params.
- Response: `{"id":"…","result":{…}}` or `{"id":"…","error":{"kind":"<kind>"}}`. Responses may come in any order.
- Event: `{"event":"<name>","network":"…",…}`, with no `id`.
- Times are integers in Unix milliseconds, in fields ending `Ms`. Cursors are opaque strings. Unknown fields are ignored on both sides.
- The first request is `hello` `{"protocol":1,"storeKey":"<64 hex>","app":"<Pllum version>"}` → `{"protocol":1,"version":"<helper version>","commit":"<git SHA>","networks":["whatsapp"]}`. Any other version → error `outdated`, and Pllum stops the helper. Before `hello`, every other request is `badRequest`.

**Error kinds** (the only values of `kind`): `badRequest`, `unknownMethod`, `unknownNetwork`, `notConnected` (not signed in), `offline` (signed in, no connection now), `notFound`, `notOnNetwork` (a `beeper:` line), `refused` (WhatsApp turned it down before it was sent; safe to retry), `unclear` (it may have happened), `rateLimited`, `tooLarge`, `mediaGone`, `unsupported`, `storeKey`, `outdated`, `timeout`, `internal`, and, for networks signed in through a website, `checkpoint`, `pinWrong`, `pinLimit` (see Additions below).

**States** (`status` and the `state` event): `notConnected`, `linking`, `connecting`, `catchingUp`, `live`, `offline`, `reconnectNeeded`, `outdated`, `banned`.

**Chat:** `{"id","kind":"single"|"group","title"|null,"phone"|null,"lastActivityMs","lastMessageID"|null,"lastFromMe","unread","muted","readOnly","participants":[{"id","name"|null,"phone"|null,"isSelf"}],"historyFromMs"|null,"historyComplete"}`. `title` is the group's subject, else WhatsApp's name for the person (his phone's contact name, then their push name), else null. `participants` holds at most 64. `muted` is read against the clock: a mute his phone set until a time reads muted until then and unmuted after, and the helper sends `changed` when it lapses.

**Message:** `{"id","chatID","senderID","senderName"|null,"fromMe","timeMs","text"|null,"kind":"text"|"photo"|"video"|"voiceNote"|"sticker"|"file"|"notice","hidden","deleted","edited","replyTo"|null,"media":[{"kind","width"|null,"height"|null,"hasFile"}],"reactions":[{"emoji","senderID","fromMe"}],"seenMs"|null}`.

**Methods:**

| Method | Params (besides `network`) | Result |
|---|---|---|
| `hello` | `protocol`, `storeKey`, `app` | `protocol`, `version`, `commit`, `networks` |
| `shutdown` | — | `{}`, then exits |
| `status` | — | `state`, `selfChatID`\|null |
| `login` | `method`: `"qr"`, or `"session"` with `session: {"<cookie name>": "<value>"}` (every value a string; an empty or mistyped `session` is `badRequest`) | `{}`; progress comes as `qr` and `state` events (for `session`, `state` events only; a failure is `state` `notConnected` with a `kind`) |
| `cancelLogin`, `logout` | — | `{}` |
| `nudge` | — | `{}`: reconnect now if not connected (after sleep) |
| `chats` | `sinceMs`, `cursor`?, `limit` (≤200) | `items: [Chat]`, `cursor`? (newest activity first) |
| `chat` | `chatID` | `chat: Chat`\|null |
| `messages` | `chatID`, `before`?, `limit` (≤200) | `items: [Message]` oldest first, `hasMore`, `oldestCursor`?. A page that reaches the oldest stored line (`hasMore` false) of a chat that is not `historyComplete` also asks the network to fetch older lines, if it can: at most once per chat per 10 s, never while one is running; the answer does not wait for it |
| `search` | `text`?, `chatIDs`, `fromMe`?, `senderID`?, `afterMs`?, `beforeMs`?, `limit` (≤50), `cursor`? | `items: [Message]` newest first, `cursor`? |
| `findChats` | `query`, `limit` (≤20) | `items: [Chat]` |
| `send` | `chatID`, `text`, `replyTo`?, `file`? (an absolute path), `fileName`?, `clientID`? (the caller's own key for this send) | `messageID` (made before sending; returns at once). A repeat with the `clientID` of a send that is sending or sent returns that send's `messageID` and sends nothing more, even while not connected; one whose send failed makes a new send, which the key then names |
| `sendStatus` | `messageID`, or `clientID` (the key a `send` was made with) instead | `state`: `sending`\|`sent`\|`failed`\|`unknown`, `kind`?, `lineID`?; asked by `clientID`, also `messageID` of the send the key names (none, and `unknown`, when it names none). Both or neither is `badRequest`. `lineID` is the ID of the line the send became, once the network has it and when it differs from the sent ID; only on `sent` |
| `react` | `chatID`, `messageID`, `emoji` (`""` removes), `transactionID` | `{}` |
| `markRead` | `chatID`, `through`? (a message ID; null = newest incoming) | `{}` |
| `markUnread` | `chatID` | `{}` |
| `setMuted` | `chatID`, `muted` | `{}` |
| `media` | `chatID`, `messageID` | `path` (a file under `connect/media/`) |
| `import` | `beeperChatID`, `chatID`\|null, `chat: {kind, title?, phone?}`, `messages: [ImportMessage]`, `replaces`? | `added`, `matched`, `skipped` |
| `beeperChats` | — | `map: {beeperChatID: chatID}` |
| `lineIDs` | `beeperIDs: [String]` (≤1000) | `map: {beeperID: lineID}` |
| `counts` | — | `chats`, `messages`, `imported`, `matched`, `unplacedChats`, `oldestMs`\|null, `nearMisses` (optional) |
| `signIn` | — | `url`, `donePattern`, `cookieDomain`, `required: [String]`, `optional: [String]`, `userAgent`; `unsupported` for a network with no sign-in page |
| `chatByParticipant` | `participantID` | `chat: Chat`\|null: the one-to-one with that person (never a group, never an unplaced chat; the most recently active if several) |
| `encryptedHistory` | — | `state`: `noBackup`\|`needsPIN`\|`restored`\|`skipped`; `unsupported` for a network with none |
| `restoreHistory` | `pin` (a string; `""` skips; missing is `badRequest`) | `{}`; errors `pinWrong` (with `attemptsRemaining`?), `pinLimit`; `unsupported` for a network with none |

`counts.nearMisses` is a measure, a count only: per network, the Beeper-only lines (`source` beeper) in placed chats that have a direct line on the same side (`fromMe`) within 2000 ms, none of which has the same content as the Beeper line under the matcher's `MatchKey` rules. It reads text to compare and returns a number, never text, IDs or JIDs, and changes nothing about matching. A helper that does not send it is read as 0.

`ImportMessage`: `{"beeperID","networkID"|null,"fromMe","senderID"|null,"senderName"|null,"timeMs","text"|null,"kind","hidden","deleted","replyToBeeperID"|null,"media":[{"kind","width","height"}],"reactions":[{"emoji","senderID","fromMe"}]}`. A null `chatID` makes or extends the unplaced chat `unplaced:<beeperChatID>`; `replaces` names an unplaced chat whose rows are dropped once this placed import has them. An exact-time, one-to-one twin counts as matched: a copy whose text differs (a mention, say) from a direct line that carries no Beeper ID, on the same side at the same non-zero `timeMs`, when each is the only such line at that time in the chat and both have text or share a media kind.

**Events:** `session` `{session}`: the full named cookie set of a network signed in through a website, sent when it differs from the last one sent (or the one `login` was given), at most once a minute, and never a set missing a required cookie; `state` `{state, kind?}`; `qr` `{code, expiresMs}`; `history` `{chats, messages, done}` (counts while the link-time sync arrives; `done` after a 100% batch or 120 s with none); `changed` `{}` (something to re-read; at most one per 500 ms per network).

**Additions for networks signed in through a website** (Instagram, Messenger). All are additive, so the protocol stays 1; Pllum and the helper ship together.

- `hello`'s `networks` lists `"instagram"` and `"messenger"` when the helper runs them.
- `signIn`: Pllum shows `url` in a web view with `userAgent`, waits for an address matching `donePattern` (a regular expression on the full URL), reads the `required` and `optional` cookies of `cookieDomain`, and sends them as `login` with `method:"session"`. The helper presents the same user agent to Meta, so the window and the helper look like one browser.
- Error kind `checkpoint`: Meta wants him to confirm it is him (a challenge, checkpoint or consent page). It is also a `state` `kind`, on state `reconnectNeeded`. The helper makes no further requests until he signs in again.
- How a `login` with a session ends: `badRequest` at once (and `state` `notConnected` `badRequest`) when a required cookie is missing or empty, before anything is asked of Meta; else `{}`, then `state` `connecting`, and `catchingUp` when the socket subscribes, `live` at its first update or 60 s on. A checkpoint is `reconnectNeeded` with kind `checkpoint`, an ended session `reconnectNeeded`, a suspended account `banned`; Meta's error 1357053 and a redirect loop count as an ended session. Any other failure is `notConnected` with its kind: a `nudge` retries `offline` and `timeout` with the cookies held; `rateLimited` is retried by the helper when the block ends; anything else waits for a new `login`. A failed inbox reload after Meta asks for a resnapshot is `offline` (with `rateLimited` while a block is in force), retried the same way. The cookies are held in memory only (the latest rotated set once Meta rotates them), so Pllum sends `login` on every start; with the client's saved state for that session the helper resumes from the saved sequence ID, with no request before the socket, and loads the inbox otherwise. A checkpoint, ended session or suspension is kept in the store (its kind and a SHA-256 of the required cookies of every version of the session sent to Pllum, never their values): at start, and on a `login` matching any of them, the helper reports that state again and asks Meta nothing; a `login` with a different session (a new sign-in) clears it. A rate limit is kept too (its end and its count), so a restart neither asks before the block ends nor starts the doubling over.
- Instagram's IDs: a chat is its thread key in decimal (for a one-to-one, the other person's FBID); a line is Meta's message ID (`mid.$…`) unchanged; a participant is an Instagram user ID (IGID). `selfChatID` is null.
- Instagram's writes (every one through the pace, as are the reads they need: the thread's IDs, an upload, a media fetch and an expired photo's refresh; while a block is in force each is refused with its kind). `send` needs state `live`, `catchingUp` or `offline` (else `notConnected`) and answers at once with `otid:<offline threading ID>`; the send goes in the background, in order. A file is uploaded first, then any text goes as its own message, then the photo or video (JPEG, PNG, MP4 or QuickTime, up to 16 MB; anything else is `unsupported`): two lines, as Instagram shows them, and `lineID` is the file's. `sendStatus`: `sent` with `lineID` (Meta's message ID) once Meta answers or the send's echo arrives; `failed` `refused` when nothing reached Meta or Meta said no (`unclear` when the text of a text-and-file went and the file did not); `sending` while it may have reached Meta with no answer (a timeout); a send still `sending` when the helper starts is `unknown` from then on and is never sent again (a later echo still makes it `sent`). No write is asked again after Meta answered or may have. A repeated `send` with the same `clientID` gives the same ID and sends nothing, unless the first failed. `react` with an empty emoji removes his reaction; a `transactionID` already held sends nothing; a `beeper:` line is `notOnNetwork`. `markRead` names the newest unread line at or before `through`. `markUnread` is kept in the helper's store only (Instagram has no such write). `setMuted` mutes for ever or unmutes. `media` fetches a photo or sticker once from its CDN address; an address the CDN no longer takes is refreshed with one page of history, then the answer is `mediaGone`.
- Error kinds `pinWrong` and `pinLimit` are `restoreHistory`'s only. `pinWrong` may carry `attemptsRemaining`, an integer: `{"id":"…","error":{"kind":"pinWrong","attemptsRemaining":2}}`. No other error carries it.
- `status` may carry `encrypted`: `notRegistered`\|`connecting`\|`live`\|`failed` (Messenger only; a network that has no end-to-end encryption leaves it out).
- Pace: the helper leaves a random 2 to 4 s between requests to Meta. After `rateLimited` it makes none for 15 minutes, then 30, 60, 120 and 240 on each further one (a success starts over); after `checkpoint`, `reconnectNeeded` or `banned` none until a new `login`. A success during a rate-limit block does not end it.

**Go interface each network implements** (`internal/network`):

```go
type Network interface {
	Name() string                                     // "whatsapp"
	Start(ctx context.Context) error                  // connects if signed in; returns at once
	Status() Status                                   // State, SelfChatID
	Login(ctx context.Context, p LoginParams) error // LoginParams{Method string; Session Session}
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

A network may also implement these optional interfaces, which `server` finds with a type assertion (`internal/network`):

```go
type SignInPager interface{ SignIn() meta.SignInPage }
type OlderPages interface {
	FetchOlder(ctx context.Context, chatID string) (added int, err error)
}
type EncryptedHistory interface {
	HistoryState() string
	RestoreHistory(ctx context.Context, pin []byte) error
}
```

A network without one answers `unsupported` to its method (`OlderPages` is simply not called). `FetchOlder` sends `changed` itself when it adds lines or finds the start of a chat.

`Session` is `map[string]string` whose `String`, `GoString` and `Format` all give `[session redacted]`, so no `fmt` verb prints a cookie.

Reads (`chats`, `chat`, `messages`, `search`, `findChats`, `chatByParticipant`) and the Beeper calls (`import`, `beeperChats`, `lineIDs`, `counts`) are answered by `server` from `store` for every network; a network only writes into the store.

