# Integration Test Case Matrix

## Legend

- Status: `Implemented` or `Planned`
- Area: `Load`, `Dump`, `RoundTrip`, `Error handling`

## Cases

| Case ID | Area | Test Method | Covered Types | Input Data | Expected Result | Status |
|---|---|---|---|---|---|---|
| IT-LDR-001 | Load | `setup_正常ケース_Oracleコンテナに対してFlywayを実行する_マイグレーションが完了すること` | Schema baseline | Flyway `V1`, `V2` | Oracle schema/data is reset and migrated | Implemented |
| IT-LDR-002 | Load | `execute_正常ケース_拡張Oracle型をロードする_全列値が登録されること` | NUMBER, BINARY_FLOAT, BINARY_DOUBLE, VARCHAR2, CHAR, NVARCHAR2, NCHAR, DATE, TIMESTAMP, TSTZ, TSLTZ, INTERVAL YM/DS, RAW, CLOB, NCLOB, BLOB | `integration/load/pre/db1/*.csv`, `files/*` | Row counts and column values are loaded correctly | Implemented |
| IT-LDR-003 | Error handling | `execute_異常ケース_interval形式が不正である_ロールバックされること` | INTERVAL YM/DS | Corrupted `IT_TYPED_MAIN.csv` in temp copy | Load fails and transaction is rolled back | Implemented |
| IT-DMP-001 | Dump | `setup_正常ケース_Oracleコンテナに対してFlywayを実行する_マイグレーションが完了すること` | Schema baseline | Flyway `V1`, `V2` | Oracle schema/data is reset and migrated | Implemented |
| IT-DMP-002 | Dump | `execute_正常ケース_拡張Oracle型をダンプする_CSVとLOBファイルが出力されること` | NUMBER, BINARY_FLOAT, BINARY_DOUBLE, VARCHAR2, CHAR, NVARCHAR2, NCHAR, DATE, TIMESTAMP, TSTZ, TSLTZ, INTERVAL YM/DS, RAW, CLOB, NCLOB, BLOB | Seeded DB data | CSV and LOB files are generated and values match DB | Implemented |
| IT-DMP-003 | Dump | `execute_正常ケース_interval列をダンプする_規定フォーマットで出力されること` | INTERVAL YM/DS | Seeded DB data | Intervals are dumped as `+YY-MM` and `+DD HH:MM:SS` | Implemented |
| IT-DMP-004 | Dump | `execute_正常ケース_nullと空LOBを含むデータをダンプする_CSV表現とLOBファイル内容が期待どおりであること` | VARCHAR2, CHAR, NVARCHAR2, NCHAR, CLOB, NCLOB, BLOB | Seeded DB data + additional rows with `NULL`, `EMPTY_CLOB()`, `EMPTY_BLOB()` | NULL is dumped as empty CSV cells, empty LOBs are dumped as zero-byte files and referenced as `file:` | Implemented |
| IT-RT-001 | RoundTrip | `execute_正常ケース_拡張Oracle型をロード後にダンプする_件数と全列値が一致すること` | Same as IT-LDR-002 + IT-DMP-002 | Load fixture then dumped output | Input and dumped data match by row/column normalization rules | Implemented |
| IT-MY-LDR-001 | Load | `setup_正常ケース_MySQLコンテナに対してFlywayを実行する_マイグレーションが完了すること` | Schema baseline | Flyway `V1`, `V2` | MySQL schema/data is reset and migrated | Implemented |
| IT-MY-LDR-002 | Load | `execute_正常ケース_MySQL型をロードする_全列値が登録されること` | Numeric, ENUM/SET/JSON, DATE/TIME/DATETIME/TIMESTAMP/YEAR, BLOB/TEXT | `integration/mysql/load/pre/db1/*.csv`, `files/*` | Row counts and column values are loaded correctly | Implemented |
| IT-MY-ERR-001 | Error handling | `execute_異常ケース_日時形式が不正である_ロールバックされること` | DATETIME/TIMESTAMP | Corrupted `IT_TYPED_MAIN.csv` in temp copy | Load fails and transaction is rolled back | Implemented |
| IT-MY-DMP-001 | Dump | `execute_正常ケース_MySQL型をダンプする_CSVとLOBファイルが出力されること` | Numeric, ENUM/SET/JSON, DATE/TIME/DATETIME/TIMESTAMP/YEAR, BLOB/TEXT | Seeded DB data | CSV and LOB files are generated and values match DB | Implemented |
| IT-MY-RT-001 | RoundTrip | `execute_正常ケース_ロード後にダンプする_件数と全列値が一致すること` | Same as IT-MY-LDR-002 + IT-MY-DMP-001 | Load fixture then dumped output | Input and dumped data match by row/column normalization rules | Implemented |
| IT-MS-LDR-001 | Load | `execute_正常ケース_SQLServer型をロードする_全列値が登録されること` | Numeric, character, date/time, XML, VARBINARY | `integration/sqlserver/load/pre/db1/*.csv`, `files/*` | Row counts and column values are loaded correctly | Planned |
| IT-MS-DMP-001 | Dump | `execute_正常ケース_SQLServer型をダンプする_CSVとLOBファイルが出力されること` | Numeric, character, date/time, XML, VARBINARY | Seeded DB data | CSV and LOB files are generated and values match DB | Planned |
| IT-MS-RT-001 | RoundTrip | `execute_正常ケース_ロード後にダンプする_件数と全列値が一致すること` | Same as IT-MS-LDR-001 + IT-MS-DMP-001 | Load fixture then dumped output | Input and dumped data match by row/column normalization rules | Planned |

## Excluded Types

| Type | Reason |
|---|---|
| BFILE | External file and Oracle directory dependency |
| ROWID, UROWID | Internal locator semantics |
| LONG, LONG RAW | Legacy types outside project scope |
| OBJECT, VARRAY, NESTED TABLE | Not supported by current CSV/DBUnit mapping |
| XMLTYPE | XML is tested via CLOB strategy |
| Oracle native JSON | JSON is tested via CLOB strategy |
