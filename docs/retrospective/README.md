# Retrospective

*skydeval, April 2026, with a postscript from October 2026*

## Why I built it

I had an operational need for this. I've been building a
privacy-focused ATProto client, for Bluesky but not only Bluesky, and
it grew into a whole suite, including its own AppView. When I was
choosing a portfolio project, I wanted something that would stay
useful after the portfolio was done. horizon-firehose was that: a Rust
firehose consumer that could replace the Python one in my AppView.

## How it was built

I started with a design document and had it adversarially reviewed
before writing any code. From there I directed Claude Code to turn the
design into a working program, making the calls along the way that
kept the project on the path the design laid out.

I was new to Rust. I'd ported two existing projects to it before this,
which was an easy way in, but this was the first time I built
something in Rust from nothing to shipped.

proto-blue, Doll's ATProto Rust SDK, is the project's only ATProto
dependency. While building on it I filed four issues upstream and had
a pull request merged. That was my first contribution to someone
else's open source project.

## Adversarial review

The design went through five review rounds, and the code went through
two. For the code reviews I used fresh sessions with no project folder,
no context on the scope, and no history at all. The reviewer had no
stake in anything, which made it much better at finding real problems
than my older habit of asking the same session to attack its own
ideas.

I'd planned on three or four code reviews. I stopped at two when the
reviewer started reaching more than it was finding.

## Learning as I went

There were plenty of moments where I didn't understand the problem in
front of me. Asking for an explanation and then making the decision
myself worked well. A few times something went straight over my head;
I waited for the explanation, or stopped the work early to ask, more
often on this project than any before it. Once I understood the
trade-off I could make a better call.

## What I'd do differently

Bring Docker in earlier. It came at the end, and running the real
image sooner would have given me confidence in the code sooner.

## Where it ended up

It runs in my AppView alongside the Python consumer, both publishing to
the same stream. I chose a parallel run over a full cutover for now,
because I still edit the firehose consumers often and the Python one is
quicker to change. When that settles down I'll revisit the cutover. I'm
proud of this one, and I like it more.

Watching it build and run on the server, fully working, for the first
time was pure joy.

## Postscript: October 2026

Six months in, production found something neither review did. Two
relays stalled at the same time, and the SDK's own reconnect logic
re-dialled using a cursor from the original connection, almost a day
old. The relays replayed tens of millions of events and overflowed the
shared stream.

The fix was to give the SDK a function that builds the URL with the
current cursor on every dial, plus a guard in the publisher that drops
anything already published. The details are in
[the design docs](../design/cursors-and-relays.md#every-dial-uses-the-current-cursor).
The lesson: reviews check the code you wrote, but the paths that run
inside a dependency deserve the same scrutiny.
