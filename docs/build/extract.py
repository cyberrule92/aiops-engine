#!/usr/bin/env python3
"""Extract ```mermaid fenced blocks from the markdown into numbered .mmd files,
and emit a copy of the markdown with each block replaced by an <img> placeholder
token DIAGRAM_NN so the HTML builder can swap in rendered PNGs."""
import re, pathlib

SRC = pathlib.Path("../SOLUTION_DESIGN.md")
OUT = pathlib.Path("diagrams")
OUT.mkdir(exist_ok=True)

text = SRC.read_text()
lines = text.splitlines()
out_lines, blocks = [], []
i, n = 0, 0
while i < len(lines):
    if lines[i].strip() == "```mermaid":
        body = []
        i += 1
        while i < len(lines) and lines[i].strip() != "```":
            body.append(lines[i]); i += 1
        i += 1  # skip closing fence
        n += 1
        (OUT / f"diagram_{n:02d}.mmd").write_text("\n".join(body) + "\n")
        out_lines.append(f"@@DIAGRAM_{n:02d}@@")
        blocks.append(n)
    else:
        out_lines.append(lines[i]); i += 1

pathlib.Path("body.md").write_text("\n".join(out_lines))
print(f"extracted {n} mermaid diagrams")
