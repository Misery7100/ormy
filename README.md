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
- Clickhouse;
- BigQuery (partial implementation).

Extensions:

- MeiliSearch;
- S3;
- Redlock (custom implementation).

## TO DO

- [ ] Add indexing support for mongodb service;
- [ ] Add indexing support for firestore service;
- [ ] Add indexing support for clickhouse service;
- [ ] Join support for clickhouse service;
- [ ] Non-context clients for redis service;
- [ ] Check `pytest-benchmark` for performance testing;
- [ ] Check `pytest-meilisearch` for MeiliSearch testing;
- [ ] Extend unit tests.
