# Ormy the Ferret
<!-- markdownlint-disable MD033 -->

<p align="center">
  <img src="/images/ormy_1.png" alt="Ormy the Ferret" height="400">
</p>

Pydantic-compatible ORM (and ORM-like) wrappers of various kinds.

## Features

Services:

- MongoDB;
- Firestore;
- Redis;
- Clickhouse (with partial async support);
- BigQuery (partial implementation).

Extensions:

- MeiliSearch;
- S3;
- Redlock (custom implementation).

## TO DO

- [ ] Improve logging;
- [x] Non-context clients for redlock extension;
- [x] Non-context clients for meilisearch extension;
- [ ] Non-context clients for redis service;
- [x] Cached clients for mongodb service;
- [ ] Check `pytest-benchmark` for performance testing;
- [ ] Check `pytest-meilisearch` for MeiliSearch testing;
- [ ] Extend unit tests.
