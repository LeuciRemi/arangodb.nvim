# Reproduce the demo

This is a real ArangoDB server with fictional data, not an HTTP stub. Run the
commands below from the repository root. Requirements: Docker Compose, Python 3,
Neovim >= 0.10, and an installed `snacks.nvim`.

## Start and seed

```bash
docker compose -f demo/compose.yaml up -d
python3 demo/seed.py
nvim -u demo/init.lua -i NONE
```

The container uses ArangoDB 3.12.5, binds only to `127.0.0.1:18529`, and stores
its database in temporary memory-backed storage. Its public demo credentials are
`root` / `demo-only`. No existing database server is used. The `demo` database
contains four users, three projects, four contribution edges, and the named graph
`team`. Rerunning the seed replaces those fixture documents, including edits made
during the recording; it does not remove additional documents you created.

The minimal Neovim configuration loads Snacks from
`stdpath("data") .. "/lazy/snacks.nvim"`. Set `SNACKS_PATH` to your installation
if it is elsewhere:

```bash
SNACKS_PATH=/path/to/snacks.nvim nvim -u demo/init.lua -i NONE
```

Try `:ArangoBrowse demo`, select `users`, open `alice`, change her role, and
`:write`. Use `:ArangoGraph demo` with graph `team` and start vertex `users/alice`
to explore contributions. AQL examples are in the main README.

## Record the GIF

The recorder launches Neovim, attaches to its public UI API, and renders the
actual grid events with Pillow. It drives the real plugin against the demo
server, edits Alice's role to `Maintainer`, saves it, executes a bound AQL query,
and switches its results to a table. The captions are added by the recorder;
the editor contents come from Neovim. AQL history is disabled for this session.
No desktop screenshot or personal Neovim configuration is captured.

Install recording-only dependencies in a virtual environment (they are not
plugin dependencies):

```bash
python3 -m venv /tmp/arangodb-demo-venv
/tmp/arangodb-demo-venv/bin/pip install Pillow msgpack
python3 demo/seed.py
DEMO_FONT=/path/to/JetBrainsMonoNerdFontMono-Regular.ttf \
  /tmp/arangodb-demo-venv/bin/python demo/record.py
```

Use a monospaced Nerd Font with box-drawing glyphs. The default font path is
`/usr/share/fonts/TTF/JetBrainsMonoNerdFontMono-Regular.ttf`. The output is
`doc/assets/demo.gif`; review PNG snapshots and Neovim messages under
`/tmp/arangodb-demo-*` before committing it. Run the seed again before each
recording to restore Alice's initial role.

## Stop and discard

```bash
docker compose -f demo/compose.yaml down
```

Stopping the container discards its temporary database. Start and seed again to
restore the fixture. This container is intended only for local demonstrations.
