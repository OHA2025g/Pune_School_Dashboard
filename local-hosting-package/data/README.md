## Data folder

This repository intentionally does **not** commit raw data dumps (MongoDB `.bson` files) or Excel source files (`.xlsx`) to GitHub.

### What you should do locally

- **MongoDB**: restore your local dump into your local Mongo instance (recommended), then start the backend.
- **Excel**: keep source Excel files locally if you need to re-run ETL/import jobs.

### Why this is excluded from GitHub

- Raw dumps can contain **sensitive data**.
- Large binary files make the repo heavy and are harder to review in PRs.


