#!/usr/bin/env python3
"""Fetch all WordPress posts, save raw JSON, and export a clean CSV."""

from __future__ import annotations

import argparse
import csv
import html
import json
import sys
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen


DEFAULT_POSTS_URL = "https://thesophian.com/wp-json/wp/v2/posts"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


class RequestFailure(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        url: str,
        status_code: int | None = None,
        reason: str | None = None,
    ) -> None:
        super().__init__(message)
        self.url = url
        self.status_code = status_code
        self.reason = reason


class PartialFetchError(RuntimeError):
    def __init__(self, message: str, partial_posts: list[dict[str, Any]]) -> None:
        super().__init__(message)
        self.partial_posts = partial_posts


def build_request_url(url: str, params: dict[str, Any] | None = None) -> str:
    query = f"?{urlencode(params)}" if params else ""
    return f"{url}{query}"


def fetch_json(
    url: str,
    params: dict[str, Any] | None = None,
    *,
    request_label: str | None = None,
) -> tuple[Any, dict[str, str]]:
    request_url = build_request_url(url, params)
    if request_label:
        print(f"Requesting {request_label}: {request_url}", file=sys.stderr)
    else:
        print(f"Requesting: {request_url}", file=sys.stderr)

    request = Request(
        request_url,
        headers={
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        },
    )

    try:
        with urlopen(request) as response:
            payload = response.read().decode("utf-8")
            headers = dict(response.info().items())
    except HTTPError as exc:
        print(
            f"HTTPError {exc.code} {exc.reason} for {request.full_url}",
            file=sys.stderr,
        )
        raise RequestFailure(
            f"HTTP error {exc.code} for {request.full_url}",
            url=request.full_url,
            status_code=exc.code,
            reason=exc.reason,
        ) from exc
    except URLError as exc:
        print(f"URLError for {request.full_url}: {exc.reason}", file=sys.stderr)
        raise RequestFailure(
            f"Network error for {request.full_url}: {exc.reason}",
            url=request.full_url,
            reason=str(exc.reason),
        ) from exc

    try:
        return json.loads(payload), headers
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON received from {request.full_url}") from exc


def collect_paginated_posts(posts_url: str, per_page: int, pause_seconds: float) -> list[dict[str, Any]]:
    page = 1
    total_pages = None
    posts: list[dict[str, Any]] = []

    while total_pages is None or page <= total_pages:
        try:
            data, headers = fetch_json(
                posts_url,
                {"page": page, "per_page": per_page},
                request_label=f"posts page {page}",
            )
        except RequestFailure as exc:
            if exc.status_code in {400, 404}:
                print(
                    f"Stopping pagination cleanly at page {page}; WordPress reported no more pages.",
                    file=sys.stderr,
                )
                break
            raise PartialFetchError(str(exc), posts) from exc

        if not isinstance(data, list):
            raise PartialFetchError(
                f"Expected a list of posts on page {page}, got {type(data).__name__}",
                posts,
            )

        if not data:
            print(f"Stopping pagination at page {page}; no posts returned.", file=sys.stderr)
            break

        posts.extend(data)

        if total_pages is None:
            raw_total_pages = headers.get("X-WP-TotalPages") or headers.get("x-wp-totalpages")
            if raw_total_pages:
                total_pages = int(raw_total_pages)

        if total_pages is not None:
            print(f"Fetched page {page}/{total_pages} ({len(data)} posts)", file=sys.stderr)
        else:
            print(f"Fetched page {page} ({len(data)} posts)", file=sys.stderr)
        page += 1

        if pause_seconds > 0 and (total_pages is None or page <= total_pages):
            time.sleep(pause_seconds)

    return posts


def fetch_taxonomy_map(base_api_url: str, resource: str) -> dict[int, str]:
    endpoint = f"{base_api_url.rstrip('/')}/{resource.lstrip('/')}"
    page = 1
    total_pages = None
    mapping: dict[int, str] = {}

    while total_pages is None or page <= total_pages:
        try:
            data, headers = fetch_json(
                endpoint,
                {"page": page, "per_page": 100},
                request_label=f"{resource} page {page}",
            )
        except RequestFailure as exc:
            if exc.status_code in {400, 404}:
                break
            raise
        if not isinstance(data, list):
            raise RuntimeError(f"Expected a list from {endpoint}, got {type(data).__name__}")
        if not data:
            break

        for item in data:
            term_id = item.get("id")
            name = item.get("name")
            if isinstance(term_id, int) and isinstance(name, str):
                mapping[term_id] = name

        if total_pages is None:
            raw_total_pages = headers.get("X-WP-TotalPages") or headers.get("x-wp-totalpages") or "1"
            total_pages = int(raw_total_pages)

        page += 1

    return mapping


def fetch_author_map(base_api_url: str) -> dict[int, str]:
    endpoint = f"{base_api_url.rstrip('/')}/users"
    page = 1
    total_pages = None
    mapping: dict[int, str] = {}

    while total_pages is None or page <= total_pages:
        try:
            data, headers = fetch_json(
                endpoint,
                {"page": page, "per_page": 100},
                request_label=f"users page {page}",
            )
        except RequestFailure as exc:
            if exc.status_code in {400, 404}:
                break
            raise
        if not isinstance(data, list):
            raise RuntimeError(f"Expected a list from {endpoint}, got {type(data).__name__}")
        if not data:
            break

        for item in data:
            user_id = item.get("id")
            name = item.get("name") or item.get("slug")
            if isinstance(user_id, int) and isinstance(name, str):
                mapping[user_id] = name

        if total_pages is None:
            raw_total_pages = headers.get("X-WP-TotalPages") or headers.get("x-wp-totalpages") or "1"
            total_pages = int(raw_total_pages)

        page += 1

    return mapping


def clean_text(value: Any) -> str:
    if isinstance(value, dict):
        value = value.get("rendered", "")
    if value is None:
        return ""
    return " ".join(html.unescape(str(value)).split())


def ids_to_names(values: list[int], mapping: dict[int, str]) -> str:
    names = [mapping.get(value, str(value)) for value in values]
    return "|".join(names)


def build_csv_rows(
    posts: list[dict[str, Any]],
    author_map: dict[int, str],
    category_map: dict[int, str],
    tag_map: dict[int, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for post in posts:
        categories = [value for value in post.get("categories", []) if isinstance(value, int)]
        tags = [value for value in post.get("tags", []) if isinstance(value, int)]
        author_id = post.get("author")

        rows.append(
            {
                "id": post.get("id", ""),
                "date": post.get("date", ""),
                "slug": post.get("slug", ""),
                "link": post.get("link", ""),
                "title": clean_text(post.get("title")),
                "author": author_map.get(author_id, str(author_id) if author_id is not None else ""),
                "author_id": author_id if author_id is not None else "",
                "categories": ids_to_names(categories, category_map),
                "category_ids": "|".join(str(value) for value in categories),
                "tags": ids_to_names(tags, tag_map),
                "tag_ids": "|".join(str(value) for value in tags),
            }
        )

    return rows


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "id",
        "date",
        "slug",
        "link",
        "title",
        "author",
        "author_id",
        "categories",
        "category_ids",
        "tags",
        "tag_ids",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def derive_base_api_url(posts_url: str) -> str:
    parsed = urlparse(posts_url)
    path = parsed.path
    marker = "/posts"
    if marker not in path:
        raise ValueError("Posts URL must include '/posts' so taxonomy endpoints can be derived.")
    base_path = path[: path.index(marker)]
    return f"{parsed.scheme}://{parsed.netloc}{base_path}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export all WordPress posts to raw JSON and clean CSV."
    )
    parser.add_argument(
        "--posts-url",
        default=DEFAULT_POSTS_URL,
        help=f"WordPress posts endpoint (default: {DEFAULT_POSTS_URL})",
    )
    parser.add_argument(
        "--raw-json",
        default="wordpress_posts_raw.json",
        help="Output path for the raw posts JSON export",
    )
    parser.add_argument(
        "--csv",
        default="wordpress_posts_clean.csv",
        help="Output path for the cleaned CSV export",
    )
    parser.add_argument(
        "--per-page",
        type=int,
        default=100,
        help="Posts to request per page (WordPress commonly supports up to 100)",
    )
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=0.5,
        help="Optional delay between paginated requests",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_api_url = derive_base_api_url(args.posts_url)
    raw_json_path = Path(args.raw_json)
    csv_path = Path(args.csv)

    posts: list[dict[str, Any]] = []
    author_map: dict[int, str] = {}
    category_map: dict[int, str] = {}
    tag_map: dict[int, str] = {}
    exit_code = 0

    try:
        posts = collect_paginated_posts(args.posts_url, args.per_page, args.pause_seconds)
        author_map = fetch_author_map(base_api_url)
        category_map = fetch_taxonomy_map(base_api_url, "categories")
        tag_map = fetch_taxonomy_map(base_api_url, "tags")
    except PartialFetchError as exc:
        posts = exc.partial_posts
        exit_code = 1
        print(f"Fetch stopped after partial success: {exc}", file=sys.stderr)
    except Exception as exc:
        exit_code = 1
        print(f"Fetch failed: {exc}", file=sys.stderr)

    rows = build_csv_rows(posts, author_map, category_map, tag_map)
    write_json(raw_json_path, posts)
    write_csv(csv_path, rows)

    print(f"Saved {len(posts)} posts to {raw_json_path}")
    print(f"Saved {len(rows)} cleaned rows to {csv_path}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
