# Chainmorph - Go meets ETL through method chaining

> Go library that allows you to create a step-wise ETL pipeline
> using Go 1.27+
>
> This is all possible thanks to the ability to create generic methods
> that define their own generic constraints!

The name 'chain' comes from the fact that I've built the basics of this
library because I focused very hard on making the public API to use
method-chaining as much as possible because it looks cool, and I like
doing cool-stuff. The second part, 'morph', is because every method
morphs the previous step in some way via closures.

## Why this library even exists

Recently I tried to finish a project that I had already resigned on
doing called [RivenBot](https://www.github.com/Riven-of-a-Thousand-Servers)
which is essentially a Charlamagne wanna-be Discord bot that gives
Destiny 2 players insights on their raid statistics. The initial work
for this project involved in processing several GBs of initial data,
courtesy from [D2Asun](https://d2.asun.co/pgcr.html), the goat, into
my own historical database. This process is technically ETL processing,
where I am reading from compressed datasources of historical Destiny 2
data, creating my own internal mappings and saving them in my database.
I have been working on this processing for the past two months and
I've come at a cross roads that whatever I'm doing looks like shit
and is hard to test because of how much gibberish and scattered logic
I've written.

Hence Chainmorph's existence. When I first saw ThePrimagen's video on
why he hates Go now it intrigued me exactly what kind of change was
Go 1.27 making that he hated it so much. Turns out that this change,
lettings methods define their own generics constraints, was a banger
of a change and made this whole library possible! Therefore, here it is.
