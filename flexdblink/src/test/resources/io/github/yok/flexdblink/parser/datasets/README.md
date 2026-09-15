# Dataset parser fixtures

`valid` contains the four supported formats. Each has one `TBL` row with `ID=1`;
`NAME` identifies its format so priority tests can verify which file was selected.
YML extension tests copy the YAML fixture as `TBL.yml`.

`malformed` contains intentionally invalid CSV, JSON, YAML, and XML for failure
propagation tests. `sparse` introduces columns in later rows. `xml-selection`
contains two distinct tables for file-selection tests. `unsupported` contains a
text file that the factory must ignore.

Tests copy these read-only fixtures into a fresh temporary directory. The CSV
round-trip tests still use the production CSV writer to generate their output.

`csv-values` covers unquoted empty NULL fields, quoted empty strings, literal `null`, quotes, commas,
backslashes, whitespace, and Unicode.
