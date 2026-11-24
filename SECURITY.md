# Security policy

## Reporting a vulnerability

Please report it privately, not in a public issue. Use GitHub's private reporting: open this repository's **Security** tab and choose **Report a vulnerability**. Only the maintainer sees the report.

If that button isn't there, open an ordinary issue that says only that you have a security report and asks for a private way to send it. Leave out every detail until then.

Say what you found, the commit you found it on, and how to reproduce it. Never include anyone's real messages, sessions or keys: invented data shows a bug just as well.

A fix lands on `main`.

## Supported versions

Only `main` is supported. Pllum builds the helper from the single commit pinned in its `connect.lock`, which is always a commit on `main`, so a fix ships by landing on `main` and moving that pin. Older commits get no fixes.

## Scope

The helper itself: its protocol handling (PROTOCOL.md), its encrypted store and sealed media, and how it talks to WhatsApp, Instagram, Messenger and LinkedIn. A bug in an upstream library it uses (whatsmeow, mautrix-meta, mautrix-linkedin) is worth reporting here too when the helper is the way it's reached, and to that project as well.
