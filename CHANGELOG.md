# Changelog

All notable changes to python-relations-pymysql are recorded here, newest first. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow [SemVer](https://semver.org/).

## [Unreleased]

### Changed
- `make new` works on the ticket branch you're on: it writes VERSION and the changelog section there and commits, makes no branch of its own, and refuses on main, since we branch from the ticket. `B=` and `BRANCH_PREFIX` are gone (relations-dil/python-relations#98).

## [0.6.19] - 2026-10-26

### Added
- Jenkins releases on a merge to `main`: a `release` stage (only on `main`) runs `make release`, which does nothing for a version that is already released and otherwise does the test release, the release and the tag. It pushes the tag with the `github` credential, as Leroy Jenkins, one build at a time, with a two hour timeout (relations-dil/python-relations#80). The package builds run in a copy with only the files they use mounted read-only, so nothing root-owned is left in the workspace, and `make tag` stops on a failure instead of ignoring it. The release scripts work with the older git on the Jenkins host.
- `make help` lists every target with what it does (each target has a `## description` after it), and is what a bare `make` runs.
- Release tooling, the same as relations-dil's (relations-dil/python-relations#77): `make new` starts a release, `make version` sets the version, `make release-check` says where a release is up to and what would stop it, `make release` does what's left (checks, TestPyPI and a clean install from it, PyPI, the tag), `make dist-check` builds and checks the distributions, and `make test-release` tests the scripts against a throwaway repo and fake indexes. The work is in `scripts/`.

### Changed
- Requires relations-sql 0.6.11 and relations-mysql 0.6.6, so a field's `length` is honored in the DDL (`VARCHAR(n)`, 255 by default) and a length-only change generates a column change (relations-dil/python-relations-mysql#17, relations-dil/python-relations-sql#30).
- `make release-check` and `make release` don't run `build`, `test`, `lint` and `setup` again: the Jenkins stages have just run them before the release stage, so it was the same work twice. Run them yourself before checking by hand, as `make release-check` is only the checks and `dist-check` now (relations-dil/python-relations#90).
- Every `docker run` in the `Makefile` uses `--rm`, and a replaced database container is removed with its volume (`docker rm --force -v`), so builds and test runs leave no stopped containers or volumes behind.
- `testpypi` and `pypi` run in the current `python` image with `build` and `twine` installed on the fly, so the custom `gaf3/pypi` image is no longer used.

Shipping as 0.6.16, with relations-dil 0.6.16.

### Added
- A `VERSION` file as the one place the version is set: the `Makefile` reads it and mounts it, `testpypi` and `pypi` pass it as `BUILD_VERSION`, and `setup.py` reads `BUILD_VERSION` or the file.

### Changed
- A key stored in a dict field (`child_inject` in relations-dil 0.6.16) is queried by its path in that field, instead of a column that doesn't exist: new `retrieve_record` hands the record to `retrieve_field`, which looks an injected field up in the field it's stored in (as the extracted column if it's been extracted), and `sort` does the same. Counting, retrieving, updating and deleting by the key work, and mass updates through it are refused.

## [0.6.18] - 2026-10-14

### Changed
- A duplicate (MySQL error 1062, which a repeated primary key is too) raises `relations.UniqueError` from create and update, with the driver's `pymysql.err.IntegrityError` as its cause, through the new `Source.write`. Any other integrity error is left as it is.
- "none retrieved" and "more than one retrieved" are `relations.NotFoundError` and `relations.MultipleError` (same messages, still `ModelError`s). Needs relations-dil 0.6.19 (relations-dil/python-relations#78).

## [0.6.17] - 2026-10-14

### Changed
- Needs relations-dil 0.6.18 and relations-mysql 0.6.5: a string id (caller supplied, a non-int id) is created, read, updated, deleted and tied with its primary key. The source needed no change, the work is in relations-dil, relations-sql and relations-mysql.

## [0.6.15] - 2026-06-08

- Fixed count and retrieve for many-to-many ties searched through attributes: flat joins now use `COUNT(DISTINCT ...)` and `SELECT DISTINCT` so a model tied to several matching siblings is not repeated.
- Updated the relations-dil and relations-sql requirements.

## [0.6.14] - 2026-06-07

- Isolated the test MySQL container per build by making `NETWORK` overridable and naming the container after it.
- Cleaned up the test container and network after each run.

## [0.6.13] - 2026-06-06

- Added many-to-many ties support: ties are created, retrieved, updated and deleted along with models, and ties are included in count and retrieve queries.
- Made `Source` extend `relations_sql.SOURCE` and followed the relations renames of `child_parent_ref` and `parent_id`.
- Fixed updates so an `UPDATE` is only run when there are values to set, and skipped fields with no store when inserting.
- Moved requirements and `setup.py` to `relations-dil`, `relations-sql` and `relations-mysql`.

## [0.6.11] - 2022-08-07

- Fixed PyPI packaging details, naming the project `relations-pymysql` in `PYPI.md`.
- Filled in `README.md` with the project overview and examples.

## [0.6.10] - 2022-08-07

- Added PyPI publishing: `LICENSE.txt`, `PYPI.md`, and `testpypi` and `pypi` make targets.
- Changed packaging so the setup step no longer installs dependencies from git.

## [0.6.9] - 2022-06-02

- Added a lock around connection creation and changed it to close and drop connections from dead threads instead of reusing them.

## [0.6.8] - 2022-06-02

- Connections left by finished threads are now reused by new threads.

## [0.6.7] - 2022-04-30

- Fixed the test MySQL wait script from looping by adding a host lookup and exit on error.
- Updated the `relations-mysql` requirement.

## [0.6.6] - 2022-03-14

- Updated the requirements to newer relations, relations-sql and relations-mysql releases that fix migration issues, and extended the tests.

## [0.6.5] - 2022-03-13

- Updated the requirements to newer relations, relations-sql and relations-mysql releases for familial access; no `Source` code changed.

## [0.6.4] - 2022-03-10

- Upgraded the test database to MySQL 8.
- Updated the `relations-mysql` requirement.

## [0.6.3] - 2022-02-18

- Renamed labels to titles: `labels` became `titles`, `model._label` became `model._titles`, and `relations.Labels` became `relations.Titles`.

## [0.6.2] - 2021-11-21

- Moved the project to the relations-dil organization and prepared for PyPI by renaming the package and requirements to `python-relations-*`.
- Dependencies now install from the organization's repositories.

## [0.6.1] - 2021-11-13

- Renamed `Source` methods to the new relations naming: `init`, `define`, `create`, `retrieve`, `count`, `update`, `delete` and `labels`, plus `retrieve_field`, `retrieve_record`, `update_field` and `update_record`.
- Renamed `definition_convert` and `migration_convert` to `definition` and `migration`.

## [0.6.0] - 2021-11-12

- Rebuilt the `Source` on the `relations-sql` and `relations-mysql` packages, so queries and DDL are generated by `INSERT`, `SELECT`, `UPDATE`, `DELETE` and `TABLE` objects instead of hand-built strings.
- Models now use `SCHEMA` and `STORE` in place of `DATABASE` and `TABLE`, and `execute` accepts a `relations_sql` object or SQL text.

## [0.5.10] - 2021-08-27

- Added support for set fields: set values are sorted when searched and compared as JSON using `CAST(... AS JSON)`.
- Sorted values for `in`, `ne`, `has`, `any` and `all` so results are consistent.

## [0.5.9] - 2021-08-22

- Added list searching on JSON fields with the `has`, `any` and `all` operators using `JSON_CONTAINS` and `JSON_LENGTH`.

## [0.5.8] - 2021-08-19

- Made the `Source` thread-aware by keeping one connection per thread, created on demand through `connections`.
- Cleanup now closes every connection created by the `Source`.

## [0.5.7] - 2021-08-16

- Renamed the migration tracking table to `_relations_migration` with a `stamp` column.

## [0.5.6] - 2021-08-12

- Added `load` and `list` helpers for reading migration SQL files and pairing definition and migration files by timestamp.

## [0.5.5] - 2021-08-11

- Changed `int` fields to be defined as `BIGINT` instead of `INTEGER`.

## [0.5.4] - 2021-08-08

- Added migrations: `execute` runs SQL, `migrate` applies definition and migration files and tracks them in a table, and definitions and migrations can be converted to MySQL SQL.
- Renamed `database` to `schema` (constructor argument, `model.SCHEMA`) and added `KIND = "mysql"`.
- Table and column definitions can now be generated from plain dicts, with `column_define` and `extract_define`.

## [0.5.3] - 2021-06-17

- Fixed empty `in` and `ne` lists, which now produce `FALSE` and `TRUE` conditions instead of invalid SQL.
- Fixed title searches against parents that match nothing.

## [0.5.2] - 2021-06-16

- Updated to the relations release that added apply and access; no `Source` code changed.

## [0.5.1] - 2021-06-15

- Fixed `INDEX` definitions, which quoted field names incorrectly.

## [0.5.0] - 2021-06-14

- Reworked extract so a field's `extract` is a mapping of paths to kinds, creating one generated column per path named `<field>__<path>` with a matching type.
- Searches and title searches on extracted paths now use the generated column.

## [0.4.8] - 2021-06-13

- Followed the relations change from readonly to auto fields, so auto-increment ids are marked `auto`.
- Reworked update to take a dict of changes (`field_update(field, updates, clause, values)`) and create to use `record.create`.

## [0.4.7] - 2021-06-13

- Updates now always write values through `field.export()`.

## [0.4.6] - 2021-06-02

- Added `model_count`, returning the number of records matching a model's criteria and search.

## [0.4.5] - 2021-05-31

- Added injected fields (`inject`), which are skipped when creating, updating and defining tables.
- Fields with an `attr` are now exported before being written on update.

## [0.4.4] - 2021-05-30

- Added extracted fields: a field's `extract` setting creates a generated column from a path in a JSON field.
- Renamed the JSON path helper to `walk` and passed the model into `field_define`.

## [0.4.3] - 2021-05-29

- Fields of any non-scalar kind are now stored as JSON, not just `list` and `dict`.
- Moved JSON path building into a shared `path_retrieve` helper.

## [0.4.2] - 2021-05-27

- Added searching inside JSON fields using double-underscore paths such as `field__key__0`, using `->>` extraction.
- Added the `notlike` and `null` operators.

## [0.4.1] - 2021-05-11

- Added `model_labels` to build a `relations.Labels` structure from a model's records.

## [0.4.0] - 2021-05-07

- Added searching by title text with `like`, matching `LIKE` on the model's label fields, including looking through parent models.
- Renamed the comparison operators `ge` and `le` to `gte` and `lte`.
- Added an `overflow` flag set when a limited retrieve reaches its limit, and quoted sort field names.

## [0.3.3] - 2021-03-28

- Fixed the order of query parts so `LIMIT` is applied after the `WHERE` conditions and their values.

## [0.3.2] - 2021-02-23

- Added limit and offset support on retrieve.

## [0.3.1] - 2021-02-21

- Added sorting on retrieve, applying a model's sort or order as `ORDER BY` with ascending and descending fields.

## [0.3.0] - 2021-02-20

- Added bulk create: when a model is in bulk mode, rows are inserted together without fetching ids or creating children.

## [0.2.9] - 2021-02-20

- Fixed closing of connections so a `Source` that failed to connect no longer errors on cleanup.
- Updated to the relations API that renamed the model self-instance helper to `thy`.

## [0.2.7] - 2021-02-05

- Added `float` fields, stored as `DOUBLE`.
- Fixed `int` fields to emit their `DEFAULT` clause.

## [0.2.6] - 2021-01-24

- Fields whose default is a function no longer emit a `DEFAULT` clause in table definitions.

## [0.2.5] - 2021-01-19

- Moved to the latest relations release and bumped the package version.

## [0.2.4] - 2021-01-18

- Updated to the newer relations release that supports required unique fields, and bumped the package version.

## [0.2.3] - 2021-01-18

- Added support for `list` and `dict` fields, stored as `JSON` columns and encoded and decoded on create, retrieve and update.
- Renamed the `verify` make target to `setup` in the `Makefile` and `Jenkinsfile`.

## [0.2.2] - 2021-01-18

- Added `bool` fields, stored as `TINYINT`.
- Added support for `replace` fields, which reset to their default on update when not changed.

## [0.2.1] - 2021-01-17

- Added `UNIQUE` and `INDEX` definitions to generated tables from the model's unique and index settings.

## [0.2.0] - 2021-01-17

- Added validation support, with `NOT NULL` now driven by the field's `none` setting and errors raised as `relations.ModelError`.
- Pinned a newer relations release.

## [0.1.0] - 2021-01-04

- Initial release of the MySQL Source for relations: a `Source` class built on PyMySQL that creates tables from model definitions and handles create, retrieve, update and delete.
- Included `Dockerfile`, `Makefile`, `Jenkinsfile`, `setup.py` packaging, a README and a unit test suite.
