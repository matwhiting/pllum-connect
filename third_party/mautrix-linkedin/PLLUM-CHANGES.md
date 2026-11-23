# Pllum Connect's changes to mautrix-linkedin

This folder is a copy of `go.mau.fi/mautrix-linkedin` at
`v0.2609.1-0.20261103213510-3127fea810c0` (commit `3127fea`, 2026-11-03):
its `go.mod`, `go.sum`, `LICENSE`, `LICENSE.exceptions` and `pkg/linkedingo/`,
nothing else. Pllum Connect builds against it through
`replace go.mau.fi/mautrix-linkedin => ./third_party/mautrix-linkedin` in its
own `go.mod`. It is AGPL-3.0-or-later, as the original is (see `LICENSE`), and
every file changed here says so at its top.

Every change, and nothing else:

## 1. Conversations paged by their cursor, by folder (2026-11-19, 2026-11-23)

- `pkg/linkedingo/conversations.go` gains
  `func (c *Client) GetConversationsWithNextCursor(ctx context.Context, category, cursor string) (*CollectionResponse[ConversationCursorMetadata, Conversation], error)`
  and the query ID it uses,
  `graphQLQueryIDMessengerConversationsWithNextCursor`
  (`messengerConversations.9501074288a12f3ae9e3c7ea243bccbf`, the ID of
  mautrix/linkedin PR #122).
- Why: LinkedIn ignores `lastUpdatedBefore`, so `GetConversationsUpdatedBefore`
  answers the same newest 20 conversations on every page (mautrix/linkedin
  issue #56). Pllum's read-only spike on 2026-11-19 paged the same
  by-category query (primary inbox, 20 a page) by the response's
  `metadata.nextCursor`, sent back URL-escaped as the GraphQL variable
  `nextCursor`, with this query ID: 15 pages, 300 distinct conversations.
  An empty cursor asks for the first page.
- The folder (2026-11-23): `category` goes into the query's predicate,
  `(predicateUnions:List((conversationCategoryPredicate:(category:<category>))))`,
  built by `conversationCategoryQuery`; an empty `category` is
  `PRIMARY_INBOX`, which the first version of the method asked alone. Pllum
  Connect lists `INBOX` and `ARCHIVE` with it (conversations LinkedIn returns
  carry `categories` such as `INBOX`, `PRIMARY_INBOX`, `SECONDARY_INBOX`,
  `INMAIL`, `SPAM`). A cursor belongs to the category it came from.
- Test: `pkg/linkedingo/conversations_pllum_test.go` (a fake round-tripper;
  no request leaves the machine), including that the category variable goes
  into the predicate.
- Drop it when the library pages conversations itself.

## 2. A send is tried once (2026-11-19)

- `pkg/linkedingo/request.go`: `authedRequest` gains `noRetry bool` and
  `func (a *authedRequest) WithoutRetry() *authedRequest`; in `DoRaw`, both
  `if retryCount >= 5 {` (the network-error branch and the 502/503/504
  branch) become `if retryCount >= 5 || a.noRetry {`.
- `pkg/linkedingo/messages.go`: `SendMessage` adds `.WithoutRetry()` to its
  request chain.
- Why: `DoRaw` tries a request that fails on the network, or gets a 502, 503
  or 504, up to five more times (waiting 2 s, doubling), and `SendMessage`
  went through that loop with the same `originToken` and
  `dedupeByClientGeneratedToken: false`, so one send could reach LinkedIn up
  to six times. Now a network error on a send comes back at once, and a 502
  to 504 as a `*ResponseError`. Pllum Connect reads both as unclear and never
  sends again; Pllum's outbox settles the send by the chat's lines. Every
  other request (uploads, reactions, marks, reads) keeps the library's
  retries: a repeat of those changes nothing.
- Test: `pkg/linkedingo/request_pllum_test.go` (a counting round-tripper; no
  request leaves the machine).
- Drop it when the library can send without retrying.

## 3. A heartbeat pushed back stops the stream (2026-11-19)

- `pkg/linkedingo/realtime.go`: in `runHeartbeatsLoop`, the heartbeat's
  answer is kept; its body is closed, and an answer outside 2xx stops the
  stream with `fmt.Errorf("heartbeat: %w", &ResponseError{StatusCode: …})`.
  `readRealtimeStream` returns that cause, and `realtimeConnectLoop` hands it
  to the `TransientDisconnect` handler.
- Why: the loop checked only `DoRaw`'s error, which is nil for any status but
  a 502 to 504 the library gave up on, and it never closed the body. So a
  403, 429, 999 or 302 that only the heartbeat met went unseen, and the
  heartbeat went on every minute. Pllum Connect pauses every LinkedIn request
  on a 403, 429 or 999 and ends the session on a 302; with this change its
  `TransientDisconnect` handler sees the heartbeat's answer like any other.
- Test: `pkg/linkedingo/realtime_pllum_test.go` (a fake round-tripper; no
  request leaves the machine).
- Drop it when the library reads the heartbeat's status itself.
