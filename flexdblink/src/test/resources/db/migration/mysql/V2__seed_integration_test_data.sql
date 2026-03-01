-- Seed data for MySQL dump tests

INSERT INTO IT_TYPED_MAIN
(ID, VC_COL, CHAR_COL, NVC_COL, NCHAR_COL,
 ENUM_COL, SET_COL, JSON_COL,
 TINYINT_COL, SMALLINT_COL, MEDIUMINT_COL, INT_COL, BIGINT_COL2,
 NUM_COL, BF_COL, BD_COL, BIT_COL, BOOL_COL,
 YEAR_COL, DATE_COL, TIME_COL, TS_COL, TSTZ_COL,
 RAW_COL, BINARY_COL, VARBIN_COL,
 XML_COL, TINYTEXT_COL, MEDIUMTEXT_COL, LONGTEXT_COL, CLOB_COL, NCLOB_COL,
 TINYBLOB_COL, MEDIUMBLOB_COL, LONGBLOB_COL, BLOB_COL, LOB_KIND)
VALUES
(1, 'seed-xml', 'seed-char', 'seed-nvc', 'seed-nchar',
 'alpha', 'A,B', JSON_OBJECT('kind','xml','source','seed-main'),
 1, 10, 100, 1000, 10000,
 123.456000, 1.25, 10.125, b'00000001', TRUE,
 2026, '2026-02-01', '01:02:03', '2026-02-01 01:02:03.000000', '2026-02-01 01:02:03.000000',
 X'0A0B0C21', X'31323334', X'41424344',
 '<root><kind>xml</kind><source>seed-main</source></root>', 'tiny-seed-xml', 'medium-seed-xml', 'long-seed-xml', '<root><kind>xml</kind><source>seed</source></root>', 'nclob-seed-xml',
 X'0102', X'010203', X'01020304', X'01020304', 'xml');

INSERT INTO IT_TYPED_MAIN
(ID, VC_COL, CHAR_COL, NVC_COL, NCHAR_COL,
 ENUM_COL, SET_COL, JSON_COL,
 TINYINT_COL, SMALLINT_COL, MEDIUMINT_COL, INT_COL, BIGINT_COL2,
 NUM_COL, BF_COL, BD_COL, BIT_COL, BOOL_COL,
 YEAR_COL, DATE_COL, TIME_COL, TS_COL, TSTZ_COL,
 RAW_COL, BINARY_COL, VARBIN_COL,
 XML_COL, TINYTEXT_COL, MEDIUMTEXT_COL, LONGTEXT_COL, CLOB_COL, NCLOB_COL,
 TINYBLOB_COL, MEDIUMBLOB_COL, LONGBLOB_COL, BLOB_COL, LOB_KIND)
VALUES
(2, 'seed-zip', 'seed-char', 'seed-nvc', 'seed-nchar',
 'beta', 'B,C', JSON_OBJECT('kind','zip','source','seed-main'),
 2, 20, 200, 2000, 20000,
 999.000000, 2.50, 20.250, b'00000010', FALSE,
 2026, '2026-02-02', '02:03:04', '2026-02-02 02:03:04.000000', '2026-02-02 02:03:04.000000',
 X'0D0E0F10', X'41424344', X'45464748',
 '<root><kind>zip</kind><source>seed-main</source></root>', 'tiny-seed-zip', 'medium-seed-zip', 'long-seed-zip', 'clob-seed-zip', 'nclob-seed-zip',
 X'A1A2', X'A1A2A3', X'A1A2A3A4', X'A1A2A3A4A5', 'zip');

INSERT INTO IT_TYPED_MAIN
(ID, VC_COL, CHAR_COL, NVC_COL, NCHAR_COL,
 ENUM_COL, SET_COL, JSON_COL,
 TINYINT_COL, SMALLINT_COL, MEDIUMINT_COL, INT_COL, BIGINT_COL2,
 NUM_COL, BF_COL, BD_COL, BIT_COL, BOOL_COL,
 YEAR_COL, DATE_COL, TIME_COL, TS_COL, TSTZ_COL,
 RAW_COL, BINARY_COL, VARBIN_COL,
 XML_COL, TINYTEXT_COL, MEDIUMTEXT_COL, LONGTEXT_COL, CLOB_COL, NCLOB_COL,
 TINYBLOB_COL, MEDIUMBLOB_COL, LONGBLOB_COL, BLOB_COL, LOB_KIND)
VALUES
(3, 'seed-bin', 'seed-char', 'seed-nvc', 'seed-nchar',
 'gamma', 'A,C', JSON_OBJECT('kind','bin','source','seed-main'),
 3, 30, 300, 3000, 30000,
 1.000000, 3.75, 30.375, b'00000100', TRUE,
 2026, '2026-02-03', '03:04:05', '2026-02-03 03:04:05.000000', '2026-02-03 03:04:05.000000',
 X'11121314', X'4A4B4C4D', X'4E4F5051',
 '<root><kind>bin</kind><source>seed-main</source></root>', 'tiny-seed-bin', 'medium-seed-bin', 'long-seed-bin', 'clob-seed-bin', 'nclob-seed-bin',
 X'B1B2', X'B1B2B3', X'B1B2B3B4', X'B1B2B3B4B5B6', 'bin');

INSERT INTO IT_TYPED_AUX
(ID, MAIN_ID, LABEL, PAYLOAD_XML, PAYLOAD_JSON, PAYLOAD_CLOB, PAYLOAD_BLOB, LOB_KIND)
VALUES
(11, 1, 'seed-aux-xml', '<aux><kind>xml</kind><source>seed-aux</source></aux>',
 JSON_OBJECT('kind','xml','source','seed-aux'), '<aux><kind>xml</kind><source>seed</source></aux>', X'C1C2C3', 'xml');

INSERT INTO IT_TYPED_AUX
(ID, MAIN_ID, LABEL, PAYLOAD_XML, PAYLOAD_JSON, PAYLOAD_CLOB, PAYLOAD_BLOB, LOB_KIND)
VALUES
(12, 2, 'seed-aux-zip', '<aux><kind>zip</kind><source>seed-aux</source></aux>',
 JSON_OBJECT('kind','zip','source','seed-aux'), NULL, X'D1D2D3D4', 'zip');
