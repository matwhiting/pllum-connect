# Pllum Connect's changes to mautrix-linkedin

This folder is a copy of `go.mau.fi/mautrix-linkedin` at
`v0.2609.1-0.20261103213510-3127fea810c0` (commit `3127fea`, 2026-11-03):
its `go.mod`, `go.sum`, `LICENSE`, `LICENSE.exceptions` and `pkg/linkedingo/`,
nothing else. Pllum Connect builds against it through
`replace go.mau.fi/mautrix-linkedin => ./third_party/mautrix-linkedin` in its
own `go.mod`. It is AGPL-3.0-or-later, as the original is (see `LICENSE`), and
every file changed here says so at its top.

Every change, and nothing else:

## 1. Conversations paged by their cursor (2026-11-19)

- `pkg/linkedingo/conversations.go` gains
  `func (c *Client) GetConversationsWithNextCursor(ctx context.Context, cursor string) (*CollectionResponse[ConversationCursorMetadata, Conversation], error)`
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
- Test: `pkg/linkedingo/conversations_pllum_test.go` (a fake round-tripper;
  no request leaves the machine).
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
