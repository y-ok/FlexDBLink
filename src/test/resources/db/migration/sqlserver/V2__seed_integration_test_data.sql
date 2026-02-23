-- Seed data for dump tests

INSERT INTO IT_TYPED_MAIN
(ID, VC_COL, CHAR_COL, NVC_COL, NCHAR_COL, NUM_COL, BF_COL, BD_COL,
 DATE_COL, TIME_COL, TS_COL, TSTZ_COL, RAW_COL, XML_COL, CLOB_COL, NCLOB_COL, BLOB_COL, LOB_KIND)
VALUES
(1, 'seed-xml', 'seed-char', 'seed-nvc', 'seed-nchar',
 123.456000, 1.25, 10.125,
 CAST('2026-02-01' AS DATE), CAST('01:02:03' AS TIME),
 CAST('2026-02-01 01:02:03' AS DATETIME2),
 CAST('2026-02-01 01:02:03 +00:00' AS DATETIMEOFFSET),
 0x0A0B0C21,
 CAST('<root><kind>xml</kind><source>seed-main</source></root>' AS XML),
 '<root><kind>xml</kind><source>seed</source></root>', 'nclob-seed-xml',
 0x01020304,
 'xml');

INSERT INTO IT_TYPED_MAIN
(ID, VC_COL, CHAR_COL, NVC_COL, NCHAR_COL, NUM_COL, BF_COL, BD_COL,
 DATE_COL, TIME_COL, TS_COL, TSTZ_COL, RAW_COL, XML_COL, CLOB_COL, NCLOB_COL, BLOB_COL, LOB_KIND)
VALUES
(2, 'seed-zip', 'seed-char', 'seed-nvc', 'seed-nchar',
 999.000000, 2.50, 20.250,
 CAST('2026-02-02' AS DATE), CAST('02:03:04' AS TIME),
 CAST('2026-02-02 02:03:04' AS DATETIME2),
 CAST('2026-02-02 02:03:04 +00:00' AS DATETIMEOFFSET),
 0x0D0E0F10,
 CAST('<root><kind>zip</kind><source>seed-main</source></root>' AS XML),
 'clob-seed-zip', 'nclob-seed-zip',
 0xA1A2A3A4A5,
 'zip');

INSERT INTO IT_TYPED_MAIN
(ID, VC_COL, CHAR_COL, NVC_COL, NCHAR_COL, NUM_COL, BF_COL, BD_COL,
 DATE_COL, TIME_COL, TS_COL, TSTZ_COL, RAW_COL, XML_COL, CLOB_COL, NCLOB_COL, BLOB_COL, LOB_KIND)
VALUES
(3, 'seed-bin', 'seed-char', 'seed-nvc', 'seed-nchar',
 1.000000, 3.75, 30.375,
 CAST('2026-02-03' AS DATE), CAST('03:04:05' AS TIME),
 CAST('2026-02-03 03:04:05' AS DATETIME2),
 CAST('2026-02-03 03:04:05 +00:00' AS DATETIMEOFFSET),
 0x11121314,
 CAST('<root><kind>bin</kind><source>seed-main</source></root>' AS XML),
 'clob-seed-bin', 'nclob-seed-bin',
 0xB1B2B3B4B5B6,
 'bin');

-- Aux
INSERT INTO IT_TYPED_AUX
(ID, MAIN_ID, LABEL, PAYLOAD_XML, PAYLOAD_CLOB, PAYLOAD_BLOB, LOB_KIND)
VALUES
(11, 1, 'seed-aux-xml',
 CAST('<aux><kind>xml</kind><source>seed-aux</source></aux>' AS XML),
 '<aux><kind>xml</kind><source>seed</source></aux>',
 0xC1C2C3, 'xml');

INSERT INTO IT_TYPED_AUX
(ID, MAIN_ID, LABEL, PAYLOAD_XML, PAYLOAD_CLOB, PAYLOAD_BLOB, LOB_KIND)
VALUES
(12, 2, 'seed-aux-zip',
 CAST('<aux><kind>zip</kind><source>seed-aux</source></aux>' AS XML),
 NULL,
 0xD1D2D3D4, 'zip');
