package diag

// HistoryLine is the history queue's day line: the local date, the queue's
// requests, the chats it finished, the chats still open, the minutes it was
// allowed to ask, and whether it was paused or stopped, with the kind.
// "chats left" is a number, or "unknown"; "paused" and "stopped" are "no",
// or "yes " and the kind word ("yes rateLimited"). Nothing else ever goes
// in: no chat, no ID, no text.
const HistoryLine = "history queue on %s: pages %d, chats finished %d, chats left %s, minutes running %d, paused %s, stopped %s"
