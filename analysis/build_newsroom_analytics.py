#!/usr/bin/env python3
"""Build a portfolio-safe newsroom analytics project from Jetpack + WordPress data."""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


ROOT = Path("/Users/karencolman/Documents/New project")
DOWNLOADS = Path("/Users/karencolman/Downloads")

SOURCE_FILES = {
    "posts": DOWNLOADS / "thesophian.com-posts-year-01_01_2024-12_31_2024.csv",
    "authors": DOWNLOADS / "thesophian.com-authors-year-01_01_2024-12_31_2024.csv",
    "referrers": DOWNLOADS / "thesophian.com-referrers-year-01_01_2024-12_31_2024.csv",
    "locations": DOWNLOADS / "thesophian.com-locations-country-year-01_01_2024-12_31_2024.csv",
    "clicks": DOWNLOADS / "thesophian.com-clicks-year-01_01_2024-12_31_2024.csv",
    "wordpress_raw": ROOT / "wordpress_posts_raw.json",
}

PROCESSED_DIR = ROOT / "data" / "processed"
TABLES_DIR = ROOT / "outputs" / "tables"
FIGURES_DIR = ROOT / "outputs" / "figures"
MEMO_DIR = ROOT / "outputs" / "memo"

WP_API_BASE = "https://thesophian.com/wp-json/wp/v2"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
REQUEST_PAUSE_SECONDS = 0.5


@dataclass
class ParsedDataset:
    name: str
    frame: pd.DataFrame
    inspection: dict[str, Any]


def ensure_directories() -> None:
    for directory in [PROCESSED_DIR, TABLES_DIR, FIGURES_DIR, MEMO_DIR]:
        directory.mkdir(parents=True, exist_ok=True)


def decode_text(path: Path) -> tuple[str, str]:
    raw = path.read_bytes()
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return raw.decode(encoding), encoding
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("unknown", b"", 0, 1, f"Unable to decode {path}")


def clean_string(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    if value.startswith('"') and value.endswith('"') and len(value) >= 2:
        value = value[1:-1]
    value = value.replace('""', '"')
    value = re.sub(r"\s+", " ", value).strip()
    return value


def to_snake_case(value: str) -> str:
    value = re.sub(r"[^0-9A-Za-z]+", "_", value.strip().lower())
    return re.sub(r"_+", "_", value).strip("_")


def parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    value = value.strip().replace(",", "")
    if value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def normalize_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url.strip())
    path = parsed.path or "/"
    normalized = f"{parsed.scheme}://{parsed.netloc}{path}"
    if normalized.endswith("/") and path != "/":
        return normalized
    if not normalized.endswith("/"):
        normalized += "/"
    return normalized


def extract_slug(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    return parts[-1] if parts else None


def extract_post_id_from_url(url: str | None) -> int | None:
    if not url:
        return None
    parsed = urlparse(url)
    values = parse_qs(parsed.query).get("p")
    if not values:
        return None
    return parse_int(values[0])


def split_path_levels(value: str | None, max_levels: int = 3) -> dict[str, str | None]:
    parts = [part.strip() for part in (value or "").split(" > ")]
    data: dict[str, str | None] = {}
    for idx in range(max_levels):
        data[f"level_{idx + 1}"] = parts[idx] if idx < len(parts) else None
    data["path_depth"] = len([part for part in parts if part])
    return data


def safe_domain_from_text(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip()
    if "://" in value:
        return urlparse(value).netloc or None
    if "/" in value:
        return value.split("/")[0]
    return value


def inspect_frame(
    name: str,
    frame: pd.DataFrame,
    *,
    encoding: str,
    source_path: Path,
    malformed_rows: int,
    likely_join_keys: list[str],
    description: str,
) -> dict[str, Any]:
    missing_values = {column: int(value) for column, value in frame.isna().sum().items()}
    duplicate_rows = int(frame.duplicated().sum())
    duplicate_candidates = {}
    for candidate in ["title", "post_url", "article_url", "url", "slug", "link", "post_id"]:
        if candidate in frame.columns:
            duplicate_candidates[candidate] = int(frame.duplicated(subset=[candidate]).sum())

    return {
        "dataset_name": name,
        "source_file": str(source_path),
        "description": description,
        "encoding": encoding,
        "row_count": int(len(frame)),
        "column_count": int(len(frame.columns)),
        "columns": "|".join(frame.columns),
        "malformed_rows_repaired_or_flagged": malformed_rows,
        "duplicate_rows": duplicate_rows,
        "duplicate_candidates": json.dumps(duplicate_candidates, ensure_ascii=False),
        "missing_values": json.dumps(missing_values, ensure_ascii=False),
        "likely_join_keys": "|".join(likely_join_keys),
    }


def parse_posts_export(path: Path) -> ParsedDataset:
    text, encoding = decode_text(path)
    records: list[dict[str, Any]] = []
    malformed_rows = 0

    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        if not raw_line.strip():
            continue
        if raw_line.count('"') % 2 == 1:
            malformed_rows += 1
        parts = raw_line.rsplit(",", 2)
        if len(parts) != 3:
            continue
        title, views, url = parts
        record = {
            "source_line_number": line_number,
            "title": clean_string(title),
            "views": parse_int(views),
            "post_url": url.strip() or None,
        }
        record["normalized_post_url"] = normalize_url(record["post_url"])
        record["slug"] = extract_slug(record["post_url"])
        record["is_article_like"] = bool(record["post_url"])
        records.append(record)

    frame = pd.DataFrame(records)
    inspection = inspect_frame(
        "posts",
        frame,
        encoding=encoding,
        source_path=path,
        malformed_rows=malformed_rows,
        likely_join_keys=["normalized_post_url", "slug", "title"],
        description="Jetpack yearly post-level traffic export with one row per viewed page or article URL.",
    )
    return ParsedDataset("posts", frame, inspection)


def parse_authors_export(path: Path) -> ParsedDataset:
    text, encoding = decode_text(path)
    records: list[dict[str, Any]] = []
    malformed_rows = 0

    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        if not raw_line.strip():
            continue
        if raw_line.count('"') % 2 == 1:
            malformed_rows += 1
        parts = raw_line.rsplit(",", 2)
        label: str | None = None
        views: int | None = None
        article_url: str | None = None

        if len(parts) == 3 and parts[-1].startswith("http"):
            label, views, article_url = parts[0], parse_int(parts[1]), parts[2].strip()
        else:
            parts = raw_line.rsplit(",", 1)
            if len(parts) == 2:
                label, views = parts[0], parse_int(parts[1])
                article_url = None
            else:
                continue

        label = clean_string(label)
        author_name = label
        article_title = None
        row_type = "author_total"
        if label and " > " in label:
            segments = [segment.strip() for segment in label.split(" > ")]
            author_name = segments[0]
            article_title = " > ".join(segments[1:])
            row_type = "author_article"

        record = {
            "source_line_number": line_number,
            "author_label": label,
            "author_name": author_name,
            "article_title": article_title,
            "views": views,
            "article_url": article_url,
            "normalized_article_url": normalize_url(article_url),
            "slug": extract_slug(article_url),
            "wordpress_post_id": extract_post_id_from_url(article_url),
            "row_type": row_type,
        }
        records.append(record)

    frame = pd.DataFrame(records)
    inspection = inspect_frame(
        "authors",
        frame,
        encoding=encoding,
        source_path=path,
        malformed_rows=malformed_rows,
        likely_join_keys=["wordpress_post_id", "normalized_article_url", "slug", "article_title"],
        description="Jetpack yearly author export mixing author total rows with author-to-article detail rows.",
    )
    return ParsedDataset("authors", frame, inspection)


def parse_two_column_export(path: Path, *, dataset_name: str, value_name: str, label_name: str, description: str) -> ParsedDataset:
    text, encoding = decode_text(path)
    records: list[dict[str, Any]] = []
    malformed_rows = 0

    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        if not raw_line.strip():
            continue
        parts = raw_line.rsplit(",", 1)
        if len(parts) != 2:
            malformed_rows += 1
            continue
        label, value = parts
        record = {
            "source_line_number": line_number,
            label_name: clean_string(label),
            value_name: parse_int(value),
        }
        records.append(record)

    frame = pd.DataFrame(records)
    inspection = inspect_frame(
        dataset_name,
        frame,
        encoding=encoding,
        source_path=path,
        malformed_rows=malformed_rows,
        likely_join_keys=[label_name],
        description=description,
    )
    return ParsedDataset(dataset_name, frame, inspection)


def wp_api_get(path: str, params: dict[str, Any] | None = None) -> tuple[Any, dict[str, str]]:
    url = f"{WP_API_BASE}/{path.lstrip('/')}"
    query = f"?{urlencode(params)}" if params else ""
    request = Request(
        f"{url}{query}",
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    with urlopen(request, timeout=30) as response:
        payload = response.read().decode("utf-8")
        headers = dict(response.info().items())
    return json.loads(payload), headers


def fetch_paginated_map(resource: str) -> dict[int, str]:
    page = 1
    total_pages = None
    mapping: dict[int, str] = {}
    while total_pages is None or page <= total_pages:
        try:
            data, headers = wp_api_get(resource, {"page": page, "per_page": 100})
        except HTTPError:
            break
        if not isinstance(data, list):
            break
        if not data:
            break
        for item in data:
            entity_id = item.get("id")
            label = item.get("name") or item.get("slug")
            if isinstance(entity_id, int) and isinstance(label, str):
                mapping[entity_id] = label
        if total_pages is None:
            total_pages = int(headers.get("X-WP-TotalPages", headers.get("x-wp-totalpages", "1")))
        page += 1
        if total_pages is None or page <= total_pages:
            time.sleep(REQUEST_PAUSE_SECONDS)
    return mapping


def fetch_author_map_from_embedded_posts() -> dict[int, str]:
    page = 1
    total_pages = None
    mapping: dict[int, str] = {}
    while total_pages is None or page <= total_pages:
        data, headers = wp_api_get("posts", {"page": page, "per_page": 100, "_embed": "author"})
        if not isinstance(data, list):
            break
        if not data:
            break
        for item in data:
            embedded = item.get("_embedded", {})
            authors = embedded.get("author", []) if isinstance(embedded, dict) else []
            if authors:
                author = authors[0]
                author_id = author.get("id")
                name = author.get("name") or author.get("slug")
                if isinstance(author_id, int) and isinstance(name, str):
                    mapping[author_id] = name
            elif isinstance(item.get("author"), int):
                mapping.setdefault(item.get("author"), f"author_{item.get('author')}")
        if total_pages is None:
            total_pages = int(headers.get("X-WP-TotalPages", headers.get("x-wp-totalpages", "1")))
        page += 1
        if total_pages is None or page <= total_pages:
            time.sleep(REQUEST_PAUSE_SECONDS)
    return mapping


def build_wordpress_metadata() -> tuple[pd.DataFrame, dict[str, Any]]:
    raw_posts = json.loads(SOURCE_FILES["wordpress_raw"].read_text(encoding="utf-8"))
    author_map = fetch_paginated_map("users")
    if not author_map:
        author_map = fetch_author_map_from_embedded_posts()
    category_map = fetch_paginated_map("categories")
    tag_map = fetch_paginated_map("tags")

    records = []
    for post in raw_posts:
        categories = [category_map.get(value, str(value)) for value in post.get("categories", [])]
        tags = [tag_map.get(value, str(value)) for value in post.get("tags", [])]
        link = post.get("link")
        title = post.get("title", {}).get("rendered", "") if isinstance(post.get("title"), dict) else post.get("title")
        title = unescape(re.sub(r"\s+", " ", title or "")).strip()

        published_at = pd.to_datetime(post.get("date"), errors="coerce")
        records.append(
            {
                "post_id": post.get("id"),
                "post_date": published_at,
                "slug": post.get("slug"),
                "link": link,
                "normalized_link": normalize_url(link),
                "title": title,
                "title_normalized": re.sub(r"\s+", " ", title).strip().casefold(),
                "author": author_map.get(post.get("author"), str(post.get("author"))),
                "author_id": post.get("author"),
                "categories": "|".join(categories),
                "tags": "|".join(tags),
                "publish_year": published_at.year if pd.notna(published_at) else None,
                "publish_month": published_at.to_period("M").strftime("%Y-%m") if pd.notna(published_at) else None,
                "headline_length": len(title),
                "category_count": len(categories),
                "tag_count": len(tags),
            }
        )

    frame = pd.DataFrame(records)
    inspection = {
        "dataset_name": "wordpress_article_metadata",
        "source_file": str(SOURCE_FILES["wordpress_raw"]),
        "description": "WordPress API post metadata enriched with live author, category, and tag labels.",
        "encoding": "utf-8",
        "row_count": int(len(frame)),
        "column_count": int(len(frame.columns)),
        "columns": "|".join(frame.columns),
        "malformed_rows_repaired_or_flagged": 0,
        "duplicate_rows": int(frame.duplicated().sum()),
        "duplicate_candidates": json.dumps(
            {
                "post_id": int(frame.duplicated(subset=["post_id"]).sum()),
                "normalized_link": int(frame.duplicated(subset=["normalized_link"]).sum()),
                "slug": int(frame.duplicated(subset=["slug"]).sum()),
            },
            ensure_ascii=False,
        ),
        "missing_values": json.dumps({column: int(value) for column, value in frame.isna().sum().items()}, ensure_ascii=False),
        "likely_join_keys": "normalized_link|slug|post_id|title_normalized",
    }
    return frame, inspection


def backfill_author_names_from_jetpack(metadata: pd.DataFrame, authors: pd.DataFrame) -> pd.DataFrame:
    metadata = metadata.copy()
    author_article_rows = authors.loc[
        (authors["row_type"] == "author_article") & authors["wordpress_post_id"].notna(),
        ["wordpress_post_id", "author_name"],
    ].drop_duplicates()
    author_article_rows = author_article_rows.rename(columns={"wordpress_post_id": "post_id", "author_name": "jetpack_author_name"})
    metadata = metadata.merge(author_article_rows, on="post_id", how="left")

    unresolved = metadata["author"].astype(str).str.fullmatch(r"author_\d+|\d+")
    metadata.loc[unresolved & metadata["jetpack_author_name"].notna(), "author"] = metadata.loc[
        unresolved & metadata["jetpack_author_name"].notna(), "jetpack_author_name"
    ]
    metadata = metadata.drop(columns=["jetpack_author_name"])
    return metadata


def enrich_auxiliary_frames(referrers: pd.DataFrame, locations: pd.DataFrame, clicks: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    referrers = referrers.copy()
    referrers = pd.concat(
        [referrers, referrers["referrer_path"].apply(lambda value: pd.Series(split_path_levels(value)))],
        axis=1,
    )
    referrers["referrer_domain"] = referrers["level_3"].fillna(referrers["level_2"]).fillna(referrers["level_1"]).apply(safe_domain_from_text)

    locations = locations.copy()
    total_views = locations["views"].sum()
    locations["audience_share"] = locations["views"] / total_views

    clicks = clicks.copy()
    clicks = pd.concat(
        [clicks, clicks["click_target"].apply(lambda value: pd.Series(split_path_levels(value)))],
        axis=1,
    )
    clicks["destination_text"] = clicks["level_2"].fillna(clicks["level_1"])
    clicks["destination_domain"] = clicks["destination_text"].apply(safe_domain_from_text)
    return referrers, locations, clicks


def join_posts_to_metadata(posts: pd.DataFrame, metadata: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    enriched = posts.copy()
    metadata_by_url = metadata.drop_duplicates(subset=["normalized_link"])
    metadata_by_slug = metadata.drop_duplicates(subset=["slug"])

    joined = enriched.merge(
        metadata_by_url,
        left_on="normalized_post_url",
        right_on="normalized_link",
        how="left",
        suffixes=("", "_wp"),
        indicator=True,
    )
    joined["join_method"] = joined["_merge"].map({"both": "normalized_url", "left_only": None, "right_only": None})
    joined = joined.drop(columns=["_merge"])

    missing_mask = joined["post_id"].isna() & joined["slug"].notna()
    if missing_mask.any():
        slug_join = (
            joined.loc[missing_mask, posts.columns]
            .merge(
                metadata_by_slug,
                on="slug",
                how="left",
                suffixes=("", "_wp"),
            )
        )
        slug_join["join_method"] = slug_join["post_id"].notna().map({True: "slug", False: None})
        for column in slug_join.columns:
            if column in joined.columns:
                joined.loc[missing_mask, column] = slug_join[column].values
            else:
                joined.loc[missing_mask, column] = slug_join[column].values

    joined["join_success"] = joined["post_id"].notna()
    joined["is_public_article"] = joined["join_success"] & joined["categories"].notna()
    diagnostics = joined.assign(
        mismatch_reason=lambda df: df.apply(
            lambda row: "missing_url"
            if pd.isna(row["post_url"])
            else "no_wordpress_match"
            if not row["join_success"]
            else None,
            axis=1,
        )
    )
    diagnostics = diagnostics[
        [
            "title",
            "views",
            "post_url",
            "normalized_post_url",
            "slug",
            "join_success",
            "join_method",
            "mismatch_reason",
            "post_id",
            "link",
        ]
    ]
    return joined, diagnostics


def build_author_article_bridge(authors: pd.DataFrame, metadata: pd.DataFrame) -> pd.DataFrame:
    article_rows = authors.loc[authors["row_type"] == "author_article"].copy()
    metadata_by_post_id = metadata.drop_duplicates(subset=["post_id"])
    metadata_by_slug = metadata.drop_duplicates(subset=["slug"])

    joined = article_rows.merge(
        metadata_by_post_id,
        left_on="wordpress_post_id",
        right_on="post_id",
        how="left",
        suffixes=("", "_wp"),
    )
    joined["join_method"] = joined["post_id"].notna().map({True: "post_id_from_url", False: None})

    missing_mask = joined["post_id"].isna() & joined["slug"].notna()
    if missing_mask.any():
        slug_join = article_rows.loc[missing_mask].merge(
            metadata_by_slug,
            on="slug",
            how="left",
            suffixes=("", "_wp"),
        )
        slug_join["join_method"] = slug_join["post_id"].notna().map({True: "slug", False: None})
        for column in slug_join.columns:
            joined.loc[missing_mask, column] = slug_join[column].values

    joined["join_success"] = joined["post_id"].notna()
    return joined


def build_summary_tables(
    posts_enriched: pd.DataFrame,
    authors_clean: pd.DataFrame,
    authors_joined: pd.DataFrame,
    referrers_clean: pd.DataFrame,
    locations_clean: pd.DataFrame,
    clicks_clean: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    article_posts = posts_enriched.loc[posts_enriched["join_success"]].copy()

    top_posts = (
        article_posts.sort_values(["views", "title"], ascending=[False, True])[
            ["title", "views", "author", "publish_year", "publish_month", "categories", "link"]
        ]
        .head(25)
        .reset_index(drop=True)
    )

    author_totals = authors_clean.loc[authors_clean["row_type"] == "author_total", ["author_name", "views"]].copy()
    author_performance = author_totals.sort_values("views", ascending=False).reset_index(drop=True)
    author_performance["share_of_author_export"] = author_performance["views"] / author_performance["views"].sum()

    referrer_summary = (
        referrers_clean.loc[referrers_clean["path_depth"] == 1, ["level_1", "visits"]]
        .rename(columns={"level_1": "referrer_group"})
        .sort_values("visits", ascending=False)
        .reset_index(drop=True)
    )
    referrer_summary["share_of_referrer_visits"] = referrer_summary["visits"] / referrer_summary["visits"].sum()

    country_summary = (
        locations_clean.rename(columns={"country": "country_name"})
        .sort_values("views", ascending=False)
        .reset_index(drop=True)
    )

    outbound_clicks_summary = (
        clicks_clean.loc[clicks_clean["path_depth"] == 1, ["destination_domain", "clicks"]]
        .dropna(subset=["destination_domain"])
        .sort_values("clicks", ascending=False)
        .reset_index(drop=True)
    )
    outbound_clicks_summary["share_of_clicks"] = outbound_clicks_summary["clicks"] / outbound_clicks_summary["clicks"].sum()

    monthly_trend = (
        article_posts.loc[article_posts["publish_year"] == 2024]
        .groupby("publish_month", dropna=False, as_index=False)["views"]
        .sum()
        .sort_values("publish_month")
        .rename(columns={"views": "article_views"})
    )

    category_rows = []
    for _, row in article_posts.iterrows():
        categories = [category.strip() for category in str(row["categories"]).split("|") if category.strip()]
        for category in categories:
            category_rows.append(
                {
                    "category": category,
                    "views": row["views"],
                    "post_id": row["post_id"],
                }
            )
    category_performance = pd.DataFrame(category_rows)
    if not category_performance.empty:
        category_performance = (
            category_performance.groupby("category", as_index=False)
            .agg(total_views=("views", "sum"), article_count=("post_id", "nunique"))
            .sort_values(["total_views", "article_count"], ascending=[False, False])
            .reset_index(drop=True)
        )

    joined_post_match_rate = article_posts["join_success"].mean() if len(posts_enriched) else 0.0
    unmatched_posts = posts_enriched.loc[~posts_enriched["join_success"]].copy()

    join_diagnostics = pd.DataFrame(
        [
            {
                "metric": "posts_rows",
                "value": int(len(posts_enriched)),
            },
            {
                "metric": "posts_joined_to_wordpress",
                "value": int(posts_enriched["join_success"].sum()),
            },
            {
                "metric": "posts_unmatched",
                "value": int((~posts_enriched["join_success"]).sum()),
            },
            {
                "metric": "posts_join_rate",
                "value": round(float(posts_enriched["join_success"].mean()), 4) if len(posts_enriched) else 0,
            },
            {
                "metric": "author_article_rows",
                "value": int(len(authors_joined)),
            },
            {
                "metric": "author_article_rows_joined",
                "value": int(authors_joined["join_success"].sum()),
            },
            {
                "metric": "author_article_rows_unmatched",
                "value": int((~authors_joined["join_success"]).sum()),
            },
        ]
    )

    unmatched_post_examples = unmatched_posts[["title", "views", "post_url", "slug"]].head(25).reset_index(drop=True)

    return {
        "top_posts": top_posts,
        "author_performance": author_performance,
        "referrer_summary": referrer_summary,
        "country_summary": country_summary,
        "outbound_clicks_summary": outbound_clicks_summary,
        "monthly_trend": monthly_trend,
        "category_performance": category_performance,
        "join_diagnostics": join_diagnostics,
        "unmatched_posts": unmatched_post_examples,
    }


def save_chart(frame: pd.DataFrame, *, x: str, y: str, title: str, xlabel: str, ylabel: str, filename: str, kind: str = "barh") -> None:
    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(12, 7))
    chart_data = frame.copy()
    if kind == "barh":
        chart_data = chart_data.iloc[::-1]
        plt.barh(chart_data[x], chart_data[y], color="#0F766E")
    elif kind == "bar":
        plt.bar(chart_data[x], chart_data[y], color="#B45309")
        plt.xticks(rotation=45, ha="right")
    elif kind == "line":
        plt.plot(chart_data[x], chart_data[y], marker="o", linewidth=2.5, color="#1D4ED8")
        plt.xticks(rotation=45, ha="right")
    else:
        raise ValueError(f"Unsupported chart kind: {kind}")
    plt.title(title, fontsize=16, weight="bold")
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / filename, dpi=200)
    plt.close()


def save_pie_chart(frame: pd.DataFrame, *, label: str, value: str, title: str, filename: str, top_n: int = 8) -> None:
    chart_data = frame.head(top_n).copy()
    if len(frame) > top_n:
        remainder = frame.iloc[top_n:][value].sum()
        chart_data = pd.concat(
            [
                chart_data,
                pd.DataFrame([{label: "Other", value: remainder}]),
            ],
            ignore_index=True,
        )
    plt.figure(figsize=(9, 9))
    plt.pie(chart_data[value], labels=chart_data[label], autopct="%1.1f%%", startangle=90)
    plt.title(title, fontsize=16, weight="bold")
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / filename, dpi=200)
    plt.close()


def write_markdown_inspection_report(inspections: list[dict[str, Any]]) -> None:
    lines = [
        "# Data Inspection Report",
        "",
        "This report profiles the source exports and the WordPress metadata used in the newsroom analytics project.",
        "",
    ]
    for inspection in inspections:
        lines.extend(
            [
                f"## {inspection['dataset_name']}",
                "",
                f"- Source: `{inspection['source_file']}`",
                f"- Description: {inspection['description']}",
                f"- Encoding: `{inspection['encoding']}`",
                f"- Rows: {inspection['row_count']}",
                f"- Columns: {inspection['column_count']}",
                f"- Column names: `{inspection['columns']}`",
                f"- Repaired or flagged malformed rows: {inspection['malformed_rows_repaired_or_flagged']}",
                f"- Duplicate full rows: {inspection['duplicate_rows']}",
                f"- Duplicate key candidates: `{inspection['duplicate_candidates']}`",
                f"- Missing values: `{inspection['missing_values']}`",
                f"- Likely join keys: `{inspection['likely_join_keys']}`",
                "",
            ]
        )
    (MEMO_DIR / "data_inspection_report.md").write_text("\n".join(lines), encoding="utf-8")


def write_limitations_note(join_method_summary: pd.Series) -> None:
    lines = [
        "# Limitations",
        "",
        "- Jetpack Stats is not GA4, so the project reflects page- and export-level traffic indicators rather than a full event-based analytics implementation.",
        "- Traffic and engagement metrics are limited to what Jetpack exposes in its exports; there is no session stitching, conversion funneling, or robust user-level attribution.",
        "- WordPress metadata joins are strong but imperfect. Canonical URL matching worked best, with URL-derived `?p=` post IDs and slug matching used as fallbacks where needed.",
        "- Some Jetpack rows represent homepage or non-article pages, and some exports contain quoting issues that required line-level repair during ingestion.",
        "- This is a public-safe analytical reconstruction for portfolio use, not a full internal newsroom dashboard or source-of-truth reporting environment.",
        "",
        "## Join Method Mix",
        "",
    ]
    for method, count in join_method_summary.items():
        lines.append(f"- `{method}`: {count}")
    (MEMO_DIR / "limitations.md").write_text("\n".join(lines), encoding="utf-8")


def markdown_table(frame: pd.DataFrame, rows: int = 10) -> str:
    if frame.empty:
        return "_No rows available._"
    return frame.head(rows).to_markdown(index=False)


def write_memo(summaries: dict[str, pd.DataFrame], posts_enriched: pd.DataFrame) -> None:
    top_post = summaries["top_posts"].head(1)
    top_author = summaries["author_performance"].head(1)
    top_referrer = summaries["referrer_summary"].head(1)
    top_country = summaries["country_summary"].head(1)
    top_click = summaries["outbound_clicks_summary"].head(1)

    lines = [
        "# Stakeholder Memo",
        "",
        "## Executive Summary",
        "",
        "This public-safe newsroom analytics project combines Jetpack Stats exports with WordPress metadata to show what content resonated most, which authors and sections drove traffic, where readers came from, and which outbound destinations attracted clicks during the 2024 reporting window.",
        "",
        "## Key Findings",
        "",
        f"- The highest-performing article in the export was **{top_post.iloc[0]['title']}** with **{int(top_post.iloc[0]['views']):,} views**." if not top_post.empty else "- A top-post leader could not be determined.",
        f"- The leading author row in the Jetpack author export was **{top_author.iloc[0]['author_name']}** with **{int(top_author.iloc[0]['views']):,} views**." if not top_author.empty else "- A leading author could not be determined.",
        f"- The strongest referrer group was **{top_referrer.iloc[0]['referrer_group']}** with **{int(top_referrer.iloc[0]['visits']):,} visits**." if not top_referrer.empty else "- A strongest referrer group could not be determined.",
        f"- The largest audience country was **{top_country.iloc[0]['country_name']}** with **{int(top_country.iloc[0]['views']):,} views**." if not top_country.empty else "- A top audience country could not be determined.",
        f"- The most-clicked outbound destination domain was **{top_click.iloc[0]['destination_domain']}** with **{int(top_click.iloc[0]['clicks']):,} clicks**." if not top_click.empty else "- A top outbound destination could not be determined.",
        "",
        "## Editorial And Audience Strategy Implications",
        "",
        "- Search appears to be a major acquisition channel, which suggests evergreen explainers, service journalism, and SEO-conscious headline writing matter for sustained reach.",
        "- A small set of articles and authors likely drives a disproportionate share of traffic, so editorial planning should balance breakout stories with repeatable coverage formats.",
        "- Geography and outbound click behavior can help frame future audience-development efforts and partnership opportunities without exposing sensitive user-level detail.",
        "",
        "## Limitations Of Jetpack Data",
        "",
        "- Jetpack measures are useful for directional newsroom reporting, but they are not equivalent to a full event-driven analytics stack.",
        "- The exports do not provide complete time-series traffic history, so trend analysis here is anchored to article publish month rather than daily audience sessions.",
        "- Joins between Jetpack exports and WordPress metadata are high quality but not perfect, especially for homepage rows, historical URLs, and a small number of duplicate slugs in WordPress.",
        "",
        "## Next Steps",
        "",
        "- Add a reproducible environment file and optional notebook walkthrough for future portfolio reviewers.",
        "- Consider a second public-safe layer that tracks section-level editorial performance by semester or issue cycle.",
        "- If the newsroom ever grants access to deeper analytics, expand the project with audience loyalty, search landing-page analysis, and referral-to-story conversion patterns.",
    ]
    (MEMO_DIR / "stakeholder_memo.md").write_text("\n".join(lines), encoding="utf-8")


def write_readme(summaries: dict[str, pd.DataFrame]) -> None:
    lines = [
        "# Newsroom Audience & Content Performance Analytics using WordPress + Jetpack",
        "",
        "## Business Context",
        "",
        "Student newsrooms often need actionable audience reporting without access to an enterprise analytics stack. This project demonstrates how to turn Jetpack Stats exports and WordPress metadata into portfolio-ready newsroom insights about article performance, author reach, traffic sources, audience geography, and outbound click behavior.",
        "",
        "## Data Sources",
        "",
        "- Jetpack yearly exports for posts, authors, referrers, locations, and outbound clicks",
        "- WordPress post metadata pulled from the site's WP API",
        "",
        "## Methodology",
        "",
        "1. Repair line-level CSV issues in Jetpack exports without relying on fragile header assumptions.",
        "2. Standardize column names, parse URLs and dates, and create cleaned analysis tables under `data/processed/`.",
        "3. Build a master WordPress metadata table with author, category, tag, and headline-derived features.",
        "4. Join Jetpack post-level performance to WordPress metadata using canonical URL first, then URL-derived post ID and slug fallbacks where needed.",
        "5. Produce public-safe summary tables, figures, and memo text under `outputs/`.",
        "",
        "## Key Findings",
        "",
        f"- Top post: **{summaries['top_posts'].iloc[0]['title']}** ({int(summaries['top_posts'].iloc[0]['views']):,} views)" if not summaries["top_posts"].empty else "- Top post unavailable",
        f"- Top author: **{summaries['author_performance'].iloc[0]['author_name']}** ({int(summaries['author_performance'].iloc[0]['views']):,} views)" if not summaries["author_performance"].empty else "- Top author unavailable",
        f"- Top referrer group: **{summaries['referrer_summary'].iloc[0]['referrer_group']}** ({int(summaries['referrer_summary'].iloc[0]['visits']):,} visits)" if not summaries["referrer_summary"].empty else "- Top referrer unavailable",
        f"- Largest audience country: **{summaries['country_summary'].iloc[0]['country_name']}** ({int(summaries['country_summary'].iloc[0]['views']):,} views)" if not summaries["country_summary"].empty else "- Top country unavailable",
        "",
        "## Charts",
        "",
        "- `outputs/figures/top_posts_by_views.png`",
        "- `outputs/figures/author_performance.png`",
        "- `outputs/figures/referrer_mix.png`",
        "- `outputs/figures/audience_geography.png`",
        "- `outputs/figures/outbound_clicks.png`",
        "- `outputs/figures/monthly_publish_performance.png`",
        "- `outputs/figures/category_performance.png`",
        "",
        "## Limitations",
        "",
        "- Jetpack is not GA4, so this analysis is constrained to export-level traffic signals rather than event-rich behavioral analytics.",
        "- Trend analysis is based on article publish month because the provided exports do not include a complete daily or weekly time series.",
        "- Some joins are imperfect because Jetpack and WordPress represent article URLs differently, and a few WordPress slugs are duplicated.",
        "- This repository is a public-safe analytical reconstruction, not a private newsroom dashboard.",
        "",
        "## How To Run",
        "",
        "```bash",
        "python3 -m venv .venv",
        ".venv/bin/pip install pandas matplotlib seaborn",
        ".venv/bin/python analysis/build_newsroom_analytics.py",
        "```",
        "",
        "## Portfolio-Safe Caveat",
        "",
        "The project intentionally emphasizes cleaned aggregates, editorially meaningful summaries, and stakeholder-ready visuals instead of exposing raw internal traffic logs or unnecessary sensitive detail.",
    ]
    (ROOT / "README.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    ensure_directories()

    posts = parse_posts_export(SOURCE_FILES["posts"])
    authors = parse_authors_export(SOURCE_FILES["authors"])
    referrers = parse_two_column_export(
        SOURCE_FILES["referrers"],
        dataset_name="referrers",
        value_name="visits",
        label_name="referrer_path",
        description="Jetpack yearly referral-source export with hierarchical source labels.",
    )
    locations = parse_two_column_export(
        SOURCE_FILES["locations"],
        dataset_name="locations",
        value_name="views",
        label_name="country",
        description="Jetpack yearly country-level audience export.",
    )
    clicks = parse_two_column_export(
        SOURCE_FILES["clicks"],
        dataset_name="clicks",
        value_name="clicks",
        label_name="click_target",
        description="Jetpack yearly outbound-click export with domain and destination hierarchy.",
    )

    wordpress_metadata, wordpress_inspection = build_wordpress_metadata()
    wordpress_metadata = backfill_author_names_from_jetpack(wordpress_metadata, authors.frame)
    referrers_clean, locations_clean, clicks_clean = enrich_auxiliary_frames(referrers.frame, locations.frame, clicks.frame)
    posts_enriched, post_join_diagnostics = join_posts_to_metadata(posts.frame, wordpress_metadata)
    authors_joined = build_author_article_bridge(authors.frame, wordpress_metadata)

    # Standardize processed outputs to snake_case column names.
    processed_frames = {
        "posts_clean": posts.frame.rename(columns={column: to_snake_case(column) for column in posts.frame.columns}),
        "authors_clean": authors.frame.rename(columns={column: to_snake_case(column) for column in authors.frame.columns}),
        "referrers_clean": referrers_clean.rename(columns={column: to_snake_case(column) for column in referrers_clean.columns}),
        "locations_clean": locations_clean.rename(columns={column: to_snake_case(column) for column in locations_clean.columns}),
        "clicks_clean": clicks_clean.rename(columns={column: to_snake_case(column) for column in clicks_clean.columns}),
        "wordpress_article_metadata": wordpress_metadata.rename(columns={column: to_snake_case(column) for column in wordpress_metadata.columns}),
        "article_performance_master": posts_enriched.rename(columns={column: to_snake_case(column) for column in posts_enriched.columns}),
        "author_article_bridge": authors_joined.rename(columns={column: to_snake_case(column) for column in authors_joined.columns}),
        "post_join_diagnostics": post_join_diagnostics.rename(columns={column: to_snake_case(column) for column in post_join_diagnostics.columns}),
    }

    for name, frame in processed_frames.items():
        frame.to_csv(PROCESSED_DIR / f"{name}.csv", index=False)

    summaries = build_summary_tables(posts_enriched, authors.frame, authors_joined, referrers_clean, locations_clean, clicks_clean)
    for name, frame in summaries.items():
        frame.to_csv(TABLES_DIR / f"{name}.csv", index=False)

    data_quality_summary = pd.DataFrame(
        [
            posts.inspection,
            authors.inspection,
            referrers.inspection,
            locations.inspection,
            clicks.inspection,
            wordpress_inspection,
        ]
    )
    data_quality_summary.to_csv(TABLES_DIR / "data_quality_summary.csv", index=False)

    save_chart(
        summaries["top_posts"].head(10),
        x="title",
        y="views",
        title="Top Posts By Views",
        xlabel="Views",
        ylabel="Article",
        filename="top_posts_by_views.png",
    )
    save_chart(
        summaries["author_performance"].head(10),
        x="author_name",
        y="views",
        title="Author Performance",
        xlabel="Views",
        ylabel="Author",
        filename="author_performance.png",
    )
    save_pie_chart(
        summaries["referrer_summary"],
        label="referrer_group",
        value="visits",
        title="Referrer Mix",
        filename="referrer_mix.png",
    )
    save_chart(
        summaries["country_summary"].head(10),
        x="country_name",
        y="views",
        title="Audience Geography",
        xlabel="Views",
        ylabel="Country",
        filename="audience_geography.png",
    )
    save_chart(
        summaries["outbound_clicks_summary"].head(10),
        x="destination_domain",
        y="clicks",
        title="Top Outbound Click Domains",
        xlabel="Clicks",
        ylabel="Destination Domain",
        filename="outbound_clicks.png",
    )
    save_chart(
        summaries["monthly_trend"],
        x="publish_month",
        y="article_views",
        title="Views Earned By 2024-Published Articles, By Publish Month",
        xlabel="Publish Month",
        ylabel="Views",
        filename="monthly_publish_performance.png",
        kind="line",
    )
    if not summaries["category_performance"].empty:
        save_chart(
            summaries["category_performance"].head(10),
            x="category",
            y="total_views",
            title="Category Performance",
            xlabel="Total Views",
            ylabel="Category",
            filename="category_performance.png",
        )

    write_markdown_inspection_report(
        [
            posts.inspection,
            authors.inspection,
            referrers.inspection,
            locations.inspection,
            clicks.inspection,
            wordpress_inspection,
        ]
    )
    write_memo(summaries, posts_enriched)
    write_readme(summaries)
    join_method_summary = (
        posts_enriched["join_method"].fillna("unmatched").value_counts().sort_values(ascending=False)
    )
    write_limitations_note(join_method_summary)


if __name__ == "__main__":
    main()
