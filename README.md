reddit-nntp
===========

## What is reddit-nntp?
Reddit-NNTP is an NNTP frontend for Reddit. Reddit-NNTP has two modes:
one mode for fetching posts and storing them into a spool and another
mode for serving NNTP requests through this spool. Reddit-NNTP uses
a SQLite database for its spool, so a version of the server can be
running serving NNTP requests while a background job can be run
updating the contents of the spool from reddit.

It's recommended to always have the server running and to have a job
running on a timer (e.g. cron or systemd timer) fetching new content
into the spool.

## Does reddit-nntp support posting?
Currently Reddit-NNTP does _not_ support posting and disables posting
on client connect. In the future this will probably change, but for
now I'm more interested in getting full MODE READER support in place
before adding code for posting.

## Why should I use reddit-nntp?
Reddit-NNTP is mostly RFC-3977 compliant. Reddit-NNTP advertises
capabilities. Reddit-NNTP supports almost all MODE READER
commands. Reddit-NNTP strives to be a compliant NNTP server, not a
minimal implementation. Reddit-NNTP tries to be usable as both a
standalone server and as another upstream for aggregating
newsservers. Work will continue making reddit-nntp more RFC
compliant. Reddit-NNTP mounts all of its subreddits in a parent
hierarchy which is configurable. By default this is set to `reddit`,
so if you wish to read `/r/Usenet` for example, you will find content
available in `reddit.usenet`.

Reddit-NNTP makes a great companion to Leafnode. Use Leafnode to
fetch articles from Reddit-NNTP and make sure to only grab articles in
the namespace that you've configured Reddit-NNTP to use (`reddit` by
default.) Then you can read the groups you usually read alongside
Reddit groups!

Reddit-NNTP is RFC compliant enough that you should be able to hook up
INN or any other newsserver to it, though I haven't tested it with
anything other than Leafnode and a direct connection to slrn.

## Usage

### Defaults
By default the path to the Spool DB is set to
`$HOME/.config/reddit-nntp/spool.db`.

By default the path to the config is set to
`$HOME/.config/reddit-nntp/config.toml`.

### CLI Options
```
Usage of ./reddit-nntp:
  -conf string
        path to config file (default "$HOME/.config/reddit-nntp/config.toml")
  -db string
        path to sqlite database (default "$HOME/.config/reddit-nntp/spool.db")
  -init
        initialize the database
  -subs
        get subreddits
  -update int
        update spool with contents of last n hours

```

### Building

```
go build
```

### Create a config
An example config with documented options is found at
`config.toml.example`.

### Initialize the Spool Database
```
reddit-nntp -init
```

### Populate the Spool Database for the First Time
```
reddit-nntp -subs
```

### Fetch the last n hours of content into your spool
```
reddit-nntp -update n
```

Use this to update your spool in a cron or systemd-timer.

### Serve NNTP requests (start Reddit-NNTP)
```
reddit-nntp
```

Run this as a service to use with your newsreader or another
newsserver.
