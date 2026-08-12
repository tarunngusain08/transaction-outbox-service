#!/usr/bin/env python3
"""Validate local Markdown links, diagram traceability, and Mermaid syntax."""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import unquote


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DOCUMENTS = [REPOSITORY_ROOT / "README.md", *sorted((REPOSITORY_ROOT / "docs").glob("*.md"))]
LINK_PATTERN = re.compile(r"(?<!!)\[[^]]+\]\(([^)]+)\)")
HEADING_PATTERN = re.compile(r"^#{1,6}\s+(.+?)\s*$", re.MULTILINE)
USE_CASE_PATTERN = re.compile(r"\b(?:UC-(?:P)?\d{2}|ENG-\d{2})\b")


def github_anchors(markdown: str) -> set[str]:
    anchors: set[str] = set()
    occurrences: dict[str, int] = {}
    for heading in HEADING_PATTERN.findall(markdown):
        slug = heading.strip().lower()
        slug = re.sub(r"<[^>]+>", "", slug)
        slug = slug.replace("`", "")
        slug = re.sub(r"[^\w\- ]", "", slug, flags=re.UNICODE)
        slug = re.sub(r"\s+", "-", slug)
        duplicate_index = occurrences.get(slug, 0)
        occurrences[slug] = duplicate_index + 1
        anchors.add(slug if duplicate_index == 0 else f"{slug}-{duplicate_index}")
    return anchors


def validate_links() -> list[str]:
    errors: list[str] = []
    content = {path: path.read_text(encoding="utf-8") for path in DOCUMENTS}
    anchors = {path: github_anchors(markdown) for path, markdown in content.items()}

    for source, markdown in content.items():
        for raw_target in LINK_PATTERN.findall(markdown):
            target = raw_target.strip().strip("<>")
            if target.startswith(("http://", "https://", "mailto:")):
                continue

            path_part, separator, anchor = target.partition("#")
            destination = source if not path_part else (source.parent / unquote(path_part)).resolve()
            if not destination.exists():
                errors.append(f"{source.relative_to(REPOSITORY_ROOT)}: missing link target {target}")
                continue
            if separator and anchor and destination in anchors and unquote(anchor) not in anchors[destination]:
                errors.append(f"{source.relative_to(REPOSITORY_ROOT)}: missing anchor {target}")

    return errors


def validate_traceability() -> list[str]:
    catalog = (REPOSITORY_ROOT / "docs" / "use-cases.md").read_text(encoding="utf-8")
    sequences = (REPOSITORY_ROOT / "docs" / "sequence-diagrams.md").read_text(encoding="utf-8")
    expected = set(USE_CASE_PATTERN.findall(catalog))
    documented = set(USE_CASE_PATTERN.findall(sequences))
    missing = sorted(expected - documented)
    return [f"sequence-diagrams.md is missing use cases: {', '.join(missing)}"] if missing else []


def render_mermaid() -> list[str]:
    executable = REPOSITORY_ROOT / "node_modules" / ".bin" / "mmdc"
    if not executable.exists():
        return ["Mermaid CLI is missing; run npm ci before documentation validation"]
    if not os.environ.get("PUPPETEER_EXECUTABLE_PATH"):
        return ["PUPPETEER_EXECUTABLE_PATH must point to a local Chrome/Chromium executable"]

    errors: list[str] = []
    with tempfile.TemporaryDirectory(prefix="transaction-outbox-docs-") as temporary:
        output_root = Path(temporary)
        for source in DOCUMENTS:
            if "```mermaid" not in source.read_text(encoding="utf-8"):
                continue
            output = output_root / f"{source.stem}.md"
            artefacts = output_root / source.stem
            process = subprocess.run(
                [
                    str(executable),
                    "--input", str(source),
                    "--output", str(output),
                    "--artefacts", str(artefacts),
                    "--outputFormat", "svg",
                    "--jobs", "1",
                    "--quiet",
                ],
                cwd=REPOSITORY_ROOT,
                capture_output=True,
                check=False,
                text=True,
                timeout=120,
            )
            if process.returncode != 0:
                detail = process.stderr.strip() or process.stdout.strip()
                errors.append(f"{source.relative_to(REPOSITORY_ROOT)}: Mermaid render failed: {detail}")
    return errors


def main() -> int:
    errors = [*validate_links(), *validate_traceability(), *render_mermaid()]
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1

    print(f"Validated links and Mermaid syntax in {len(DOCUMENTS)} Markdown files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
