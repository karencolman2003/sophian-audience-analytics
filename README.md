# Sophian Audience & Content Performance Analytics using WordPress + Jetpack

## Business Context

Student newsrooms often need actionable audience reporting without access to an enterprise analytics stack. This project demonstrates how to turn Jetpack Stats exports and WordPress metadata into portfolio-ready newsroom insights about article performance, author reach, traffic sources, audience geography, and outbound click behavior.

## Data Sources

- Jetpack yearly exports for posts, authors, referrers, locations, and outbound clicks
- WordPress post metadata pulled from the site's WP API

## Methodology

1. Repair line-level CSV issues in Jetpack exports without relying on fragile header assumptions.
2. Standardize column names, parse URLs and dates, and create cleaned analysis tables under `data/processed/`.
3. Build a master WordPress metadata table with author, category, tag, and headline-derived features.
4. Join Jetpack post-level performance to WordPress metadata using canonical URL first, then URL-derived post ID and slug fallbacks where needed.
5. Produce public-safe summary tables, figures, and memo text under `outputs/`.

## Key Findings

- Top post: **The Ultimate List of Icks** (373,861 views)
- Top author: **Gloria's Hymen** (373,861 views)
- Top referrer group: **Search Engines** (408,765 visits)
- Largest audience country: **United States** (330,391 views)

## Charts

- `outputs/figures/top_posts_by_views.png`
- `outputs/figures/author_performance.png`
- `outputs/figures/referrer_mix.png`
- `outputs/figures/audience_geography.png`
- `outputs/figures/outbound_clicks.png`
- `outputs/figures/monthly_publish_performance.png`
- `outputs/figures/category_performance.png`

## Limitations

- Jetpack is not GA4, so this analysis is constrained to export-level traffic signals rather than event-rich behavioral analytics.
- Trend analysis is based on article publish month because the provided exports do not include a complete daily or weekly time series.
- Some joins are imperfect because Jetpack and WordPress represent article URLs differently, and a few WordPress slugs are duplicated.
- This repository is a public-safe analytical reconstruction, not a private newsroom dashboard.

## How To Run

```bash
python3 -m venv .venv
.venv/bin/pip install pandas matplotlib seaborn
.venv/bin/python analysis/build_newsroom_analytics.py
```

## Portfolio-Safe Caveat

The project intentionally emphasizes cleaned aggregates, editorially meaningful summaries, and stakeholder-ready visuals instead of exposing raw internal traffic logs or unnecessary sensitive detail.
