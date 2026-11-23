#!/usr/bin/env python3
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Verify internal links across the built Starlight site and repo Markdown.

Modes:
  python3 scripts/verify-internal-links.py
      Checks both:
        1. Built HTML under `docs/site/dist/**/*.html` (requires `npm run build`),
        2. Source Markdown links across `README.md` and `docs/**/*.{md,mdx}`.
  python3 scripts/verify-internal-links.py --markdown-only
      Checks source Markdown links across `README.md` and `docs/**/*.{md,mdx}`
      without requiring `docs/site/dist` (invoked from `dev/tools/docs-lint`
      on every PR and local `dev/tools/ci` run).
  python3 scripts/verify-internal-links.py --self-test
      Runs hermetic positive and negative checks in a temporary directory.

Excluded:
  - External URLs (any scheme://, except our own published Starlight base URL
    `https://go-steer.github.io/k8s-lookout/...`, whose route is verified
    against the Starlight source tree, and `k8s-lookout.dev`, which is
    rejected as the wrong site domain)
  - Pure in-page anchors (#foo)
  - mailto:/tel: etc.
  - Query strings and fragment suffixes (the target path portion is checked)

Exits non-zero on any dead link.
"""
from __future__ import annotations

import argparse
import pathlib
import re
import sys
import tempfile
import urllib.parse

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
BASE = "/k8s-lookout"
PUBLISHED_SITE_PREFIX = "https://go-steer.github.io/k8s-lookout"
WRONG_DOMAIN_RE = re.compile(r"^https?://(?:www\.)?k8s-lookout\.dev(?:/|$)", re.I)

# Generated at Astro build time by `starlight-llms-txt` (astro.config.mjs).
GENERATED_ROOT_FILES = frozenset({"llms.txt", "llms-full.txt", "llms-small.txt"})

# href="..." or src="..." in built HTML — captures root-relative URLs.
HTML_URL_RE = re.compile(r'(?:href|src)="(/[^"#?]*)"')

# Inline Markdown links `[text](target)` / `![alt](target)` (including soft
# single-newline wraps inside `[text]`, optional `<...>` target wrapping, and
# optional trailing `"title"`).
MD_INLINE_LINK_RE = re.compile(
    r"!?\[(?:[^\]\n]|\n(?![ \t]*\n))*\]\(\s*(?:<([^>\n]+)>|([^)\s]+))(?:\s+(?:\"[^\"]*\"|'[^']*'))?\s*\)"
)
# Reference-style link definitions `[id]: target` (excluding `[^footnote]:`).
MD_REF_DEF_RE = re.compile(r"^\s*\[(?!\^)[^\]]+\]:\s*<?([^\s>]+)>?", re.M)
# Redirect route keys in `docs/site/astro.config.mjs`.
REDIRECT_KEY_RE = re.compile(r"""['"](/[^'"]*)['"]\s*:""")


def target_for_dist(dist: pathlib.Path, url_path: str) -> tuple[pathlib.Path, pathlib.Path]:
    """Return (dir-style, file-style) candidate paths under dist/."""
    rel = url_path[len(BASE):].lstrip("/").rstrip("/")
    if not rel:
        return dist / "index.html", dist / "index.html"
    return dist / rel / "index.html", dist / rel


def verify_dist_html(repo_root: pathlib.Path) -> tuple[int, int]:
    """Verify root-relative internal links in `docs/site/dist/**/*.html`.

    Returns (exit_code, checked_count).
    """
    dist = repo_root / "docs/site/dist"
    if not dist.is_dir():
        print(
            f"error: {dist} not found — run `npm run build` first (or pass --markdown-only)",
            file=sys.stderr,
        )
        return 2, 0

    files = sorted(dist.rglob("*.html"))
    if not files:
        print(f"error: no HTML files under {dist}", file=sys.stderr)
        return 2, 0

    broken: dict[str, set[pathlib.Path]] = {}
    checked = 0
    for f in files:
        for m in HTML_URL_RE.finditer(f.read_text(encoding="utf-8")):
            url = m.group(1)
            if not url.startswith(BASE + "/") and url != BASE:
                continue
            checked += 1
            dir_target, file_target = target_for_dist(dist, url)
            if dir_target.is_file() or file_target.is_file():
                continue
            broken.setdefault(url, set()).add(f.relative_to(dist))

    if broken:
        print(
            f"FAIL: {len(broken)} dead built-site URL(s) referenced from "
            f"{sum(len(v) for v in broken.values())} HTML page(s):"
        )
        for url in sorted(broken):
            sources = sorted(broken[url])
            print(f"  {url}")
            for src in sources[:5]:
                print(f"      referenced from: {src}")
            if len(sources) > 5:
                print(f"      ...and {len(sources) - 5} more")
        return 1, checked

    print(f"OK: {checked} built-site internal links across {len(files)} HTML pages all resolve")
    return 0, checked


def mask_code_preserving_newlines(text: str) -> str:
    """Blank out fenced code blocks and inline code spans while preserving every newline."""
    lines: list[str] = []
    fence: tuple[str, int] | None = None
    for line in text.splitlines():
        m = re.match(r"^\s*(`{3,}|~{3,})", line)
        if m:
            tok = m.group(1)
            if fence is None:
                fence = (tok[0], len(tok))
                lines.append("")
                continue
            if (
                tok[0] == fence[0]
                and len(tok) >= fence[1]
                and re.match(r"^\s*" + re.escape(tok[0]) * len(tok) + r"\s*$", line)
            ):
                fence = None
                lines.append("")
                continue
        lines.append("" if fence is not None else line)

    joined = "\n".join(lines)
    # Blank inline code spans (including spans wrapping a single newline inside a paragraph)
    # while preserving `\n` characters so line numbers remain exact.
    return re.sub(
        r"(`+)((?:(?!\1)(?!\n[ \t]*\n)[\s\S])*?)\1",
        lambda m: re.sub(r"[^\n]", " ", m.group(0)),
        joined,
    )


def load_astro_redirects(repo_root: pathlib.Path) -> set[str]:
    """Extract redirect source paths (normalized without trailing slash) from astro.config.mjs."""
    cfg = repo_root / "docs/site/astro.config.mjs"
    if not cfg.is_file():
        return set()
    text = cfg.read_text(encoding="utf-8")
    m = re.search(r"\bredirects\s*:\s*\{([^}]*)\}", text, re.S)
    if not m:
        return set()
    routes: set[str] = set()
    for km in REDIRECT_KEY_RE.finditer(m.group(1)):
        route = km.group(1).removeprefix(BASE).strip("/")
        routes.add(route)
    return routes


def is_within(child: pathlib.Path, parent: pathlib.Path) -> bool:
    try:
        child.relative_to(parent)
        return True
    except ValueError:
        return False


def starlight_route_exists(
    repo_root: pathlib.Path,
    site_docs: pathlib.Path,
    redirects: set[str],
    route_path: str,
) -> bool:
    """Return True if a Starlight site route resolves inside content/docs, public, or redirects."""
    rel = route_path
    if rel == BASE or rel.startswith(BASE + "/"):
        rel = rel[len(BASE):]
    rel = urllib.parse.unquote(rel).strip("/")
    if not rel:
        return (site_docs / "index.mdx").is_file() or (site_docs / "index.md").is_file()
    if ".." in pathlib.PurePosixPath(rel).parts:
        return False
    if rel in GENERATED_ROOT_FILES or rel in redirects:
        return True

    site_docs_resolved = site_docs.resolve()
    doc_candidates = (
        site_docs / f"{rel}.md",
        site_docs / f"{rel}.mdx",
        site_docs / rel / "index.md",
        site_docs / rel / "index.mdx",
    )
    for c in doc_candidates:
        resolved = c.resolve()
        if is_within(resolved, site_docs_resolved) and resolved.is_file():
            return True

    public_dir = (repo_root / "docs/site/public").resolve()
    if public_dir.is_dir():
        pub_target = (public_dir / rel).resolve()
        if is_within(pub_target, public_dir) and pub_target.is_file():
            return True

    return False


def iter_markdown_files(repo_root: pathlib.Path) -> list[pathlib.Path]:
    """Return `README.md` and every Markdown/MDX file under `docs/**`."""
    files: list[pathlib.Path] = []
    readme = repo_root / "README.md"
    if readme.is_file():
        files.append(readme)
    docs_dir = repo_root / "docs"
    if docs_dir.is_dir():
        for ext in ("*.md", "*.mdx"):
            for p in docs_dir.rglob(ext):
                rel_parts = p.relative_to(repo_root).parts
                if "node_modules" in rel_parts or "dist" in rel_parts:
                    continue
                files.append(p)
    return sorted(set(files))


def extract_links(text: str) -> list[tuple[int, str]]:
    """Return (1-based line_number, raw_target) pairs from Markdown text."""
    masked = mask_code_preserving_newlines(text)
    found: list[tuple[int, str]] = []
    for m in MD_INLINE_LINK_RE.finditer(masked):
        target = m.group(1) or m.group(2) or ""
        if target:
            lineno = masked.count("\n", 0, m.start()) + 1
            found.append((lineno, target))
    for rm in MD_REF_DEF_RE.finditer(masked):
        lineno = masked.count("\n", 0, rm.start()) + 1
        found.append((lineno, rm.group(1)))
    found.sort(key=lambda item: item[0])
    return found


def verify_markdown_links(repo_root: pathlib.Path) -> tuple[int, int]:
    """Verify relative and internal Markdown links in `README.md` and `docs/**`."""
    repo_root = repo_root.resolve()
    site_docs = repo_root / "docs/site/src/content/docs"
    redirects = load_astro_redirects(repo_root)
    files = iter_markdown_files(repo_root)

    broken: list[tuple[pathlib.Path, int, str, str]] = []
    checked = 0

    for md_file in files:
        in_site_docs = site_docs.is_dir() and is_within(md_file, site_docs)
        for lineno, raw_url in extract_links(md_file.read_text(encoding="utf-8")):
            if raw_url.startswith("#") or raw_url.startswith("//"):
                continue

            if WRONG_DOMAIN_RE.match(raw_url):
                checked += 1
                broken.append(
                    (
                        md_file.relative_to(repo_root),
                        lineno,
                        raw_url,
                        f"wrong docs domain k8s-lookout.dev — use {PUBLISHED_SITE_PREFIX}/",
                    )
                )
                continue

            # Check links to our own published Starlight site against source routes.
            if raw_url == PUBLISHED_SITE_PREFIX or raw_url.startswith(PUBLISHED_SITE_PREFIX + "/"):
                route = raw_url[len(PUBLISHED_SITE_PREFIX):].split("#", 1)[0].split("?", 1)[0]
                checked += 1
                if site_docs.is_dir() and not starlight_route_exists(
                    repo_root, site_docs, redirects, "/" + route.lstrip("/")
                ):
                    broken.append(
                        (
                            md_file.relative_to(repo_root),
                            lineno,
                            raw_url,
                            "published Starlight route does not exist under docs/site/",
                        )
                    )
                continue

            # Skip other external URLs / schemes (https://, mailto:, etc.).
            if re.match(r"^[a-z][a-z0-9+.-]*:", raw_url, re.I):
                continue

            path_part = urllib.parse.unquote(raw_url.split("#", 1)[0].split("?", 1)[0])
            if not path_part:
                continue

            checked += 1
            if in_site_docs and path_part.startswith("/"):
                if not starlight_route_exists(repo_root, site_docs, redirects, path_part):
                    broken.append(
                        (
                            md_file.relative_to(repo_root),
                            lineno,
                            raw_url,
                            "Starlight route does not resolve under docs/site/src/content/docs/",
                        )
                    )
                continue

            if path_part.startswith("/"):
                target = (repo_root / path_part.lstrip("/")).resolve()
            else:
                target = (md_file.parent / path_part).resolve()

            if not is_within(target, repo_root):
                broken.append(
                    (
                        md_file.relative_to(repo_root),
                        lineno,
                        raw_url,
                        "relative link escapes repository root",
                    )
                )
                continue

            if not target.exists():
                broken.append(
                    (
                        md_file.relative_to(repo_root),
                        lineno,
                        raw_url,
                        f"target {target.relative_to(repo_root)} does not exist",
                    )
                )

    if broken:
        print(f"FAIL: {len(broken)} broken internal Markdown link(s) across README.md and docs/**:")
        for src, lineno, url, reason in broken:
            print(f"  {src}:{lineno}: {url} ({reason})")
        return 1, checked

    print(f"OK: {checked} internal Markdown links across {len(files)} files all resolve")
    return 0, checked


def run_self_test() -> int:
    """Verify positive and individual negative cases for Markdown link checking."""
    with tempfile.TemporaryDirectory() as tmpdir:
        base = pathlib.Path(tmpdir)
        outside = base / "outside.md"
        outside.write_text("# Outside repo\n", encoding="utf-8")

        root = base / "repo"
        (root / "docs/site/src/content/docs/reference").mkdir(parents=True)
        (root / "docs/site/public").mkdir(parents=True)
        (root / "docs/milestones").mkdir(parents=True)
        (root / "pkg").mkdir(parents=True)

        (root / "docs/site/astro.config.mjs").write_text(
            "export default { redirects: { '/old-route': '/k8s-lookout/reference/watch/' } };\n",
            encoding="utf-8",
        )
        (root / "docs/site/src/content/docs/index.mdx").write_text("# Home\n", encoding="utf-8")
        clean_watch = (
            "# Watch\nSee [home](/) and [soft-wrapped\nredirect](/old-route).\n"
            "Ignore `[code](/does-not-exist/)` and:\n"
            "```md\n[fenced](./missing.md)\n```\n"
        )
        (root / "docs/site/src/content/docs/reference/watch.md").write_text(
            clean_watch, encoding="utf-8"
        )
        (root / "docs/repo-map.md").write_text("# Map\n", encoding="utf-8")
        (root / "pkg/real.go").write_text("package pkg\n", encoding="utf-8")
        (root / "docs/milestones/M4.md").write_text(
            "See [real](../../pkg/real.go) and [map](../repo-map.md#section).\n",
            encoding="utf-8",
        )
        clean_readme = (
            "Docs: [architecture\nmap](./docs/repo-map.md), "
            "[watch](https://go-steer.github.io/k8s-lookout/reference/watch/), "
            "[llms](https://go-steer.github.io/k8s-lookout/llms.txt).\n"
        )
        (root / "README.md").write_text(clean_readme, encoding="utf-8")

        rc, checked = verify_markdown_links(root)
        if rc != 0 or checked < 7:
            print(f"self-test FAIL: clean tree returned rc={rc}, checked={checked}", file=sys.stderr)
            return 1

        # Test each negative failure class independently so no check can regress behind another.
        negative_cases: list[tuple[str, pathlib.Path, str, str]] = [
            ("broken-rel", root / "README.md", "[dead](./docs/missing.md)\n", clean_readme),
            (
                "wrapped-broken-rel",
                root / "README.md",
                "[soft-wrapped\nlabel](./docs/missing.md)\n",
                clean_readme,
            ),
            ("repo-escape", root / "README.md", "[escape](../outside.md)\n", clean_readme),
            (
                "broken-published-url",
                root / "README.md",
                "[pub](https://go-steer.github.io/k8s-lookout/reference/nope/)\n",
                clean_readme,
            ),
            (
                "wrong-domain",
                root / "README.md",
                "[wrong](https://k8s-lookout.dev/reference/watch/)\n",
                clean_readme,
            ),
            (
                "broken-starlight-route",
                root / "docs/site/src/content/docs/reference/watch.md",
                "[dead-route](/reference/nope/)\n",
                clean_watch,
            ),
            (
                "starlight-route-traversal",
                root / "docs/site/src/content/docs/reference/watch.md",
                "[traverse](/../../../../README)\n",
                clean_watch,
            ),
        ]

        for name, target_file, bad_content, restore_content in negative_cases:
            target_file.write_text(bad_content, encoding="utf-8")
            rc_bad, _ = verify_markdown_links(root)
            target_file.write_text(restore_content, encoding="utf-8")
            if rc_bad == 0:
                print(f"self-test FAIL: negative case {name!r} was not detected", file=sys.stderr)
                return 1

    print("verify-internal-links self-test: OK")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--markdown-only",
        action="store_true",
        help="check README.md and docs/** Markdown links without requiring docs/site/dist",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="run hermetic self-tests for Markdown link verification",
    )
    args = parser.parse_args()

    if args.self_test:
        return run_self_test()

    rc_md, _ = verify_markdown_links(REPO_ROOT)
    if args.markdown_only:
        return rc_md

    rc_dist, _ = verify_dist_html(REPO_ROOT)
    if rc_dist != 0:
        return rc_dist
    return rc_md


if __name__ == "__main__":
    sys.exit(main())
