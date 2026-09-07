#!/usr/bin/env python3
"""Record the real Neovim UI grid (including Snacks) through its public RPC API.

Recording-only dependencies: Pillow and msgpack. Run from the repository root.
"""
import os
from pathlib import Path
import select
import subprocess
import time

import msgpack
from PIL import Image, ImageDraw, ImageFont

COLS, ROWS = 160, 32
CW, CH, HEADER = 10, 21, 56
FONT = ImageFont.truetype(os.environ.get("DEMO_FONT", "/usr/share/fonts/TTF/JetBrainsMonoNerdFontMono-Regular.ttf"), 16)
OUTPUT = Path("doc/assets/demo.gif")
proc = subprocess.Popen(["nvim", "--embed", "-u", "demo/init.lua", "-i", "NONE"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
unpacker = msgpack.Unpacker(raw=False)
serial = 0
responses = {}
grid = [[(" ", 0) for _ in range(COLS)] for _ in range(ROWS)]
highlights = {0: {}}
defaults = [0xD0D0D0, 0x1C1C1C]
cursor = (0, 0)
frames, durations = [], []
caption = "Browse ArangoDB without leaving Neovim"


def redraw(events):
    global cursor, grid, defaults
    for event in events:
        name, args = event[0], event[1:]
        for arg in args:
            if name == "default_colors_set":
                defaults = arg[:2]
            elif name == "hl_attr_define":
                highlights[arg[0]] = arg[1]
            elif name == "grid_clear":
                grid = [[(" ", 0) for _ in range(COLS)] for _ in range(ROWS)]
            elif name == "grid_line":
                _, row, col, cells, *_ = arg
                hl = 0
                for cell in cells:
                    if len(cell) > 1:
                        hl = cell[1]
                    for _ in range(cell[2] if len(cell) > 2 else 1):
                        if row < ROWS and col < COLS:
                            grid[row][col] = (cell[0], hl)
                        col += 1
            elif name == "grid_cursor_goto":
                cursor = (arg[1], arg[2])
            elif name == "grid_scroll":
                _, top, bottom, left, right, rows, cols = arg
                old = [r[:] for r in grid]
                for y in range(top, bottom):
                    for x in range(left, right):
                        sy, sx = y + rows, x + cols
                        grid[y][x] = old[sy][sx] if top <= sy < bottom and left <= sx < right else (" ", 0)


def pump(seconds):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if select.select([proc.stdout], [], [], max(0, end - time.monotonic()))[0]:
            data = os.read(proc.stdout.fileno(), 1048576)
            if not data:
                raise RuntimeError("Neovim exited: " + proc.stderr.read().decode())
            unpacker.feed(data)
            for message in unpacker:
                if message[0] == 1:
                    responses[message[1]] = message[2:]
                elif message[0] == 2 and message[1] == "redraw":
                    redraw(message[2])


def rpc(method, *args):
    global serial
    serial += 1
    ident = serial
    proc.stdin.write(msgpack.packb([0, ident, method, args]))
    proc.stdin.flush()
    deadline = time.monotonic() + 10
    while ident not in responses:
        pump(0.01)
        if time.monotonic() > deadline:
            raise TimeoutError(method)
    error, result = responses.pop(ident)
    if error:
        raise RuntimeError(error)
    return result


def color(value):
    return tuple((value >> shift) & 255 for shift in (16, 8, 0))


def capture(duration=100, still=None):
    im = Image.new("RGB", (COLS * CW + 32, ROWS * CH + HEADER + 16), "#10141d")
    draw = ImageDraw.Draw(im)
    draw.text((16, 9), "arangodb.nvim", font=FONT, fill="#89b4fa")
    draw.text((205, 9), caption, font=FONT, fill="#e0e6ef")
    draw.text((16, 32), "DEMO  /  fictional data  /  ArangoDB 3.12", font=FONT, fill="#8b96aa")
    for y, row in enumerate(grid):
        for x, (char, hl) in enumerate(row):
            style = highlights.get(hl, {})
            fg, bg = style.get("foreground", defaults[0]), style.get("background", defaults[1])
            if style.get("reverse"):
                fg, bg = bg, fg
            px, py = 16 + x * CW, HEADER + y * CH
            draw.rectangle((px, py, px + CW - 1, py + CH - 1), fill=color(bg))
            if char and char != " ":
                draw.text((px, py), char, font=FONT, fill=color(fg))
    cy, cx = cursor
    draw.rectangle((16 + cx * CW, HEADER + cy * CH, 16 + (cx + 1) * CW - 1, HEADER + (cy + 1) * CH - 1), outline="#89b4fa")
    frames.append(im)
    durations.append(duration)
    if still:
        im.save(still)


def pause(seconds, still=None):
    pump(seconds)
    capture(round(seconds * 1000), still)


def keys(value):
    rpc("nvim_input", value)
    pump(0.12)


def command(value):
    keys(":")
    for start in range(0, len(value), 4):
        keys(value[start:start + 4])
        capture(100)
    capture(700)
    keys("<CR>")


try:
    rpc("nvim_ui_attach", COLS, ROWS, {"rgb": True, "ext_linegrid": True})
    pump(0.5)
    rpc("nvim_command", "ArangoBrowse demo")
    pause(1)
    keys("users")
    pause(1.5, "/tmp/arangodb-demo-collections.png")
    keys("<CR>")
    pause(2, "/tmp/arangodb-demo-documents.png")
    keys("alice")
    pause(1)
    keys("<CR>")
    caption = "Open a document and edit its JSON"
    pause(2, "/tmp/arangodb-demo-document.png")
    command('%s/Engineer/Maintainer/')
    pause(1)
    caption = "Save with :write  /  revision checked by ArangoDB"
    command("write")
    pause(2, "/tmp/arangodb-demo-saved.png")
    document = rpc("nvim_buf_get_lines", 0, 0, -1, False)
    assert any("Maintainer" in line for line in document), document
    caption = "Run AQL with collection and value bind variables"
    rpc("nvim_exec_lua", 'require("arangodb").aql({ database = "demo", query = "FOR user IN @@collection\\n  FILTER user.active == @active\\n  SORT user.name\\n  RETURN { name: user.name, role: user.role }", bind_vars = { ["@collection"] = "users", active = true } })', [])
    pause(2, "/tmp/arangodb-demo-query.png")
    command("ArangoAqlExecute")
    pause(1)
    rpc("nvim_exec_lua", 'for _, w in ipairs(vim.api.nvim_list_wins()) do if vim.api.nvim_buf_get_name(vim.api.nvim_win_get_buf(w)):match("^arangodb%-aql%-result://") then vim.api.nvim_set_current_win(w); return end end; error("No AQL result window")', [])
    command("ArangoAqlResultFormat table")
    caption = "Three active users  /  switch results to a table"
    pause(3, "/tmp/arangodb-demo-results.png")
    result_lines = rpc("nvim_buf_get_lines", 0, 0, -1, False)
    assert any("Alice Martin" in line and "Maintainer" in line for line in result_lines), result_lines
    assert any("Chloe Dubois" in line for line in result_lines), result_lines
    errors = rpc("nvim_exec_lua", 'return vim.api.nvim_exec2("messages", { output = true }).output', [])
    Path("/tmp/arangodb-demo-messages.txt").write_text(errors)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    frames[0].save(OUTPUT, save_all=True, append_images=frames[1:], duration=durations, loop=0, optimize=True)
    print(f"Saved {OUTPUT}: {len(frames)} frames, {sum(durations)/1000:.1f}s, {OUTPUT.stat().st_size} bytes")
finally:
    proc.terminate()
    proc.wait(timeout=5)
