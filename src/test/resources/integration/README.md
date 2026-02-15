# Oracle Integration Test Fixtures

This directory contains fixed fixtures for Oracle Free integration tests.

## Covered Types in Integration Tests

- Numeric: `NUMBER`, `BINARY_FLOAT`, `BINARY_DOUBLE`
- Character: `VARCHAR2`, `CHAR`, `NVARCHAR2`, `NCHAR`
- Date/Time: `DATE`, `TIMESTAMP`, `TIMESTAMP WITH TIME ZONE`,
  `TIMESTAMP WITH LOCAL TIME ZONE`
- Interval: `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND`
- Binary/LOB: `RAW`, `CLOB`, `NCLOB`, `BLOB`

## Out of Scope (Explicitly Excluded)

- `BFILE`: depends on external OS file paths and Oracle directory objects.
- `ROWID`, `UROWID`: internal DB locator values, not dataset-driven business data.
- `LONG`, `LONG RAW`: legacy Oracle types, intentionally not used in this project.
- `OBJECT`, `VARRAY`, `NESTED TABLE`: user-defined collection/object types are not
  supported by current CSV/DBUnit mapping.
- `XMLTYPE`: current strategy stores XML as `CLOB`.
- Oracle native `JSON` type: current strategy stores JSON as `CLOB` text.

## Fixture Usage

- `load/pre/db1/*.csv` and `load/pre/db1/files/*` are used by `DataLoader` tests.
- Flyway migration scripts under `db/migration/oracle` create and seed tables used by
  both `DataLoader` and `DataDumper` tests.
