// mautrix-linkedin - A Matrix-LinkedIn puppeting bridge.
// Copyright (C) 2025 Sumner Evans
//
// Modified by Pllum Connect on 2026-11-19 and 2026-11-23:
// GetConversationsWithNextCursor added, taking the folder (category) to list
// (see PLLUM-CHANGES.md at the root of this copy).
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package linkedingo

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/jsontime"
)

type GraphQlResponse struct {
	Data     GraphQLData    `json:"data,omitempty"`
	Included []IncludedData `json:"included,omitempty"`
}

type GraphQLData struct {
	MessengerConversationsByCategoryQuery           *CollectionResponse[ConversationCursorMetadata, Conversation] `json:"messengerConversationsByCategoryQuery,omitempty"`
	MessengerConversationsBySyncToken               *CollectionResponse[ConversationSyncMetadata, Conversation]   `json:"messengerConversationsBySyncToken,omitempty"`
	MessengerMessagesByAnchorTimestamp              *CollectionResponse[MessageMetadata, Message]                 `json:"messengerMessagesByAnchorTimestamp,omitempty"`
	MessengerMessagesByConversation                 *CollectionResponse[MessageMetadata, Message]                 `json:"messengerMessagesByConversation,omitempty"`
	MessengerMessagingParticipantsByMessageAndEmoji *CollectionResponse[any, MessagingParticipant]                `json:"messengerMessagingParticipantsByMessageAndEmoji,omitempty"`
}

type IncludedData struct {
	Thumbnail  *VectorImage `json:"thumbnail,omitempty"`
	Content    *Content     `json:"content,omitempty"`
	EntityURN  *URN         `json:"entityUrn,omitempty"`
	Actor      *Actor       `json:"actor,omitempty"`
	Commentary *Commentary  `json:"commentary,omitempty"`
	Type       string       `json:"$type,omitempty"`
}

type Content struct {
	ArticleComponent *ArticleComponent `json:"articleComponent,omitempty"`
	ImageComponent   *ImageComponent   `json:"imageComponent,omitempty"`
}

type ArticleComponent struct {
	LargeImage *Image `json:"largeImage,omitempty"`
}
type ImageComponent struct {
	Images []Image `json:"images,omitempty"`
}

type Image struct {
	Attributes []ImageAttribute `json:"attributes,omitempty"`
}

type ImageAttribute struct {
	DetailData *ImageDetailData `json:"detailData,omitempty"`
}

type ImageDetailData struct {
	VectorImage *VectorImage `json:"vectorImage,omitempty"`
}

type Actor struct {
	Name *TextViewModel `json:"name,omitempty"`
}

type Commentary struct {
	// Text could be plain String or TextViewModel
	Text any `json:"text,omitempty"`
}

type TextViewModel struct {
	Text string `json:"text,omitempty"`
}

// CollectionResponse represents a
// com.linkedin.restli.common.CollectionResponse object.
type CollectionResponse[M, T any] struct {
	Metadata M   `json:"metadata,omitempty"`
	Elements []T `json:"elements,omitempty"`
}

// ConversationCursorMetadata represents a com.linkedin.messenger.ConversationCursorMetadata object.
type ConversationCursorMetadata struct {
	NextCursor string `json:"nextCursor,omitempty"`
}

type DeletedURN struct {
	Message      any `json:"message"`
	Conversation struct {
		EntityURN URN `json:"entityUrn"`
	} `json:"conversation"`
}

type ConversationSyncMetadata struct {
	DeletedURNs  []DeletedURN `json:"deletedUrns,omitempty"`
	NewSyncToken string       `json:"newSyncToken,omitempty"`
}

// MessageMetadata represents a com.linkedin.messenger.MessageMetadata object.
type MessageMetadata struct {
	NextCursor string `json:"nextCursor,omitempty"`
	PrevCursor string `json:"prevCursor,omitempty"`
}

// Conversation represents a com.linkedin.messenger.Conversation object
type Conversation struct {
	Title                    string                           `json:"title,omitempty"`
	EntityURN                URN                              `json:"entityUrn,omitempty"`
	LastActivityAt           jsontime.UnixMilli               `json:"lastActivityAt,omitempty"`
	LastReadAt               jsontime.UnixMilli               `json:"lastReadAt,omitempty"`
	GroupChat                bool                             `json:"groupChat,omitempty"`
	ConversationParticipants []MessagingParticipant           `json:"conversationParticipants,omitempty"`
	Read                     bool                             `json:"read,omitempty"`
	Messages                 CollectionResponse[any, Message] `json:"messages,omitempty"`
	Categories               []string                         `json:"categories,omitempty"`
}

// MessagingParticipant represents a
// com.linkedin.messenger.MessagingParticipant object.
type MessagingParticipant struct {
	ParticipantType ParticipantType `json:"participantType,omitempty"`
	EntityURN       URN             `json:"entityUrn,omitempty"`
}

type ParticipantType struct {
	Member       *MemberParticipantInfo       `json:"member,omitempty"`
	Organization *OrganizationParticipantInfo `json:"organization,omitempty"`
}

// MemberParticipantInfo represents a
// com.linkedin.messenger.MemberParticipantInfo object.
type MemberParticipantInfo struct {
	ProfileURL     string         `json:"profileUrl,omitempty"`
	FirstName      AttributedText `json:"firstName,omitempty"`
	LastName       AttributedText `json:"lastName,omitempty"`
	ProfilePicture *VectorImage   `json:"profilePicture,omitempty"`
	Pronoun        any            `json:"pronoun,omitempty"`
	Headline       AttributedText `json:"headline,omitempty"`
}

// OrganizationParticipantInfo represents a
// com.linkedin.messenger.OrganizationParticipantInfo object.
type OrganizationParticipantInfo struct {
	Name    AttributedText `json:"name,omitempty"`
	Logo    *VectorImage   `json:"logo,omitempty"`
	PageURL string         `json:"pageUrl,omitempty"`
}

func (c *Client) GetConversationsUpdatedBefore(ctx context.Context, updatedBefore time.Time) (*CollectionResponse[ConversationCursorMetadata, Conversation], error) {
	if c == nil {
		return nil, ErrClientIsNil
	}
	zerolog.Ctx(ctx).Info().
		Time("updated_before", updatedBefore).
		Msg("Getting conversations updated before")
	var response GraphQlResponse
	_, err := c.newAuthedRequest(http.MethodGet, linkedInVoyagerMessagingGraphQLURL).
		WithGraphQLQuery(graphQLQueryIDMessengerConversationsWithCursor, map[string]string{
			"mailboxUrn":        url.QueryEscape(c.userEntityURN.WithPrefix("urn", "li", "fsd_profile").String()),
			"lastUpdatedBefore": strconv.Itoa(int(updatedBefore.UnixMilli())),
			"count":             "20",
			"query":             "(predicateUnions:List((conversationCategoryPredicate:(category:PRIMARY_INBOX))))",
		}).
		Do(ctx, &response)
	if err != nil {
		return nil, err
	}
	return response.Data.MessengerConversationsByCategoryQuery, nil
}

// graphQLQueryIDMessengerConversationsWithNextCursor is the by-category
// conversations query that Pllum's read-only spike paged by its nextCursor on
// 2026-11-19 (the ID of mautrix/linkedin PR #122). Pllum Connect addition.
const graphQLQueryIDMessengerConversationsWithNextCursor = "messengerConversations.9501074288a12f3ae9e3c7ea243bccbf"

// conversationCategoryQuery is the by-category query's predicate for one
// folder (category) of LinkedIn's messaging, such as PRIMARY_INBOX, INBOX or
// ARCHIVE. Pllum Connect addition.
func conversationCategoryQuery(category string) string {
	return "(predicateUnions:List((conversationCategoryPredicate:(category:" + category + "))))"
}

// GetConversationsWithNextCursor is one page of 20 conversations of a folder
// (category: PRIMARY_INBOX, INBOX, ARCHIVE, ...), newest activity first: the
// first page with an empty cursor, and after it the page the previous one's
// Metadata.NextCursor names. A cursor belongs to the category it came from.
// An empty category is PRIMARY_INBOX, which the first version of this method
// listed alone. LinkedIn
// ignores GetConversationsUpdatedBefore's lastUpdatedBefore (it answers the
// newest 20 every time; mautrix/linkedin#56), but pages by the cursor it
// gives. Pllum Connect addition.
func (c *Client) GetConversationsWithNextCursor(ctx context.Context, category, cursor string) (*CollectionResponse[ConversationCursorMetadata, Conversation], error) {
	if c == nil {
		return nil, ErrClientIsNil
	}
	if category == "" {
		category = "PRIMARY_INBOX"
	}
	variables := map[string]string{
		"mailboxUrn": url.QueryEscape(c.userEntityURN.WithPrefix("urn", "li", "fsd_profile").String()),
		"count":      "20",
		"query":      conversationCategoryQuery(category),
	}
	if cursor != "" {
		variables["nextCursor"] = url.QueryEscape(cursor)
	}
	var response GraphQlResponse
	_, err := c.newAuthedRequest(http.MethodGet, linkedInVoyagerMessagingGraphQLURL).
		WithGraphQLQuery(graphQLQueryIDMessengerConversationsWithNextCursor, variables).
		Do(ctx, &response)
	if err != nil {
		return nil, err
	} else if response.Data.MessengerConversationsByCategoryQuery == nil {
		return nil, ErrMissingGraphQLData
	}
	return response.Data.MessengerConversationsByCategoryQuery, nil
}

func (c *Client) GetConversationsBySyncToken(ctx context.Context) (*CollectionResponse[ConversationSyncMetadata, Conversation], error) {
	if c == nil {
		return nil, ErrClientIsNil
	}
	zerolog.Ctx(ctx).Info().Msg("Getting conversations")
	req := c.newAuthedRequest(http.MethodGet, linkedInVoyagerMessagingGraphQLURL)
	mailboxUrn := url.QueryEscape(c.userEntityURN.WithPrefix("urn", "li", "fsd_profile").String())
	if c.conversationsSyncToken != "" {
		req.WithGraphQLQuery(graphQLQueryIDMessengerConversationsWithSyncToken, map[string]string{
			"mailboxUrn": mailboxUrn,
			"syncToken":  c.conversationsSyncToken,
		})
	} else {
		req.WithGraphQLQuery(graphQLQueryIDMessengerConversations, map[string]string{
			"mailboxUrn": mailboxUrn,
		})

	}
	var response GraphQlResponse
	_, err := req.Do(ctx, &response)
	if err != nil {
		return nil, err
	} else if response.Data.MessengerConversationsBySyncToken == nil {
		return nil, ErrMissingGraphQLData
	}

	newToken := response.Data.MessengerConversationsBySyncToken.Metadata.NewSyncToken
	c.conversationsSyncToken = newToken
	c.handlers.onConversationsSyncToken(ctx, newToken)
	return response.Data.MessengerConversationsBySyncToken, nil
}

func (c *Client) DeleteConversation(ctx context.Context, conversationURN URN) error {
	url := fmt.Sprintf("%s/%s", linkedInMessagingDashMessengerConversationsURL, url.QueryEscape(conversationURN.String()))
	req := c.newAuthedRequest(http.MethodDelete, url).
		WithCSRF().
		WithWebpageHeaders().
		WithXLIHeaders()
	_, err := req.Do(ctx, nil)
	return err
}

type DecoratedConversation struct {
	Result Conversation `json:"result,omitempty"`
}

type RenameConversationBody struct {
	Title string `json:"title"`
}

func (c *Client) RenameConversation(ctx context.Context, conversationURN URN, title string) error {
	url := fmt.Sprintf("%s/%s", linkedInMessagingDashMessengerConversationsURL, url.QueryEscape(conversationURN.String()))
	body := GraphQLPatchBody{Patch: Patch{Set: RenameConversationBody{Title: title}}}
	req := c.newAuthedRequest(http.MethodPost, url).
		WithCSRF().
		WithXLIHeaders().
		WithJSONPayload(body)
	_, err := req.Do(ctx, nil)
	return err
}

func (c *Client) manageParticipants(ctx context.Context, conversationURN URN, participants []URN, action string) error {
	payload := ParticipantsPayload{
		ConversationURN: conversationURN,
		Participants:    participants,
	}
	_, err := c.newAuthedRequest(http.MethodPost, linkedInMessagingDashMessengerConversationsURL).
		WithJSONPayload(payload).
		WithQueryParam("action", action).
		WithCSRF().
		WithContentType(contentTypePlaintextUTF8).
		WithXLIHeaders().
		Do(ctx, nil)
	return err
}

type ParticipantsPayload struct {
	ConversationURN URN   `json:"conversationUrn,omitempty"`
	Participants    []URN `json:"participants,omitempty"`
}

func (c *Client) AddParticipants(ctx context.Context, conversationURN URN, participants []URN) error {
	prefix := "urn:li:fsd_profile"
	for i, p := range participants {
		participants[i] = p.WithPrefix(prefix)
	}
	return c.manageParticipants(ctx, conversationURN, participants, "addParticipants")
}

func (c *Client) RemoveParticipants(ctx context.Context, conversationURN URN, participants []URN) error {
	prefix := "urn:li:msg_messagingParticipant:urn:li:fsd_profile"
	for i, p := range participants {
		participants[i] = p.WithPrefix(prefix)
	}
	return c.manageParticipants(ctx, conversationURN, participants, "removeParticipants")
}
