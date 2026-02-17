-- Seed data for dump tests (bytea/text/date/time/timestamp/timestamptz)

INSERT INTO IT_TYPED_MAIN
(ID, VC_COL, CHAR_COL, NVC_COL, NCHAR_COL, NUM_COL, BF_COL, BD_COL,
 DATE_COL, TIME_COL, TS_COL, TSTZ_COL, RAW_COL, XML_COL, CLOB_COL, NCLOB_COL, BLOB_COL, LOB_KIND)
VALUES
(1, 'seed-xml',  'seed-char', 'seed-nvc', 'seed-nchar',
 123.456000, 1.25, 10.125,
 DATE '2026-02-01', TIME '01:02:03', TIMESTAMP '2026-02-01 01:02:03', TIMESTAMPTZ '2026-02-01 01:02:03+09',
 decode('0A0B0C21', 'hex'),
 '<root><kind>xml</kind><source>seed-main</source></root>',
 '<root><kind>xml</kind><source>seed</source></root>', 'nclob-seed-xml',
 decode('01020304', 'hex'),
 'xml');

INSERT INTO IT_TYPED_MAIN
(ID, VC_COL, CHAR_COL, NVC_COL, NCHAR_COL, NUM_COL, BF_COL, BD_COL,
 DATE_COL, TIME_COL, TS_COL, TSTZ_COL, RAW_COL, XML_COL, CLOB_COL, NCLOB_COL, BLOB_COL, LOB_KIND)
VALUES
(2, 'seed-zip',  'seed-char', 'seed-nvc', 'seed-nchar',
 999.000000, 2.50, 20.250,
 DATE '2026-02-02', TIME '02:03:04', TIMESTAMP '2026-02-02 02:03:04', TIMESTAMPTZ '2026-02-02 02:03:04+09',
 decode('0D0E0F10', 'hex'),
 '<root><kind>zip</kind><source>seed-main</source></root>',
 'clob-seed-zip', 'nclob-seed-zip',
 decode('A1A2A3A4A5', 'hex'),
 'zip');

INSERT INTO IT_TYPED_MAIN
(ID, VC_COL, CHAR_COL, NVC_COL, NCHAR_COL, NUM_COL, BF_COL, BD_COL,
 DATE_COL, TIME_COL, TS_COL, TSTZ_COL, RAW_COL, XML_COL, CLOB_COL, NCLOB_COL, BLOB_COL, LOB_KIND)
VALUES
(3, 'seed-bin',  'seed-char', 'seed-nvc', 'seed-nchar',
 1.000000, 3.75, 30.375,
 DATE '2026-02-03', TIME '03:04:05', TIMESTAMP '2026-02-03 03:04:05', TIMESTAMPTZ '2026-02-03 03:04:05+09',
 decode('11121314', 'hex'),
 '<root><kind>bin</kind><source>seed-main</source></root>',
 'clob-seed-bin', 'nclob-seed-bin',
 decode('B1B2B3B4B5B6', 'hex'),
 'bin');

-- Aux
INSERT INTO IT_TYPED_AUX
(ID, MAIN_ID, LABEL, PAYLOAD_XML, PAYLOAD_CLOB, PAYLOAD_BLOB, LOB_KIND)
VALUES
(11, 1, 'seed-aux-xml', '<aux><kind>xml</kind><source>seed-aux</source></aux>',
 '<aux><kind>xml</kind><source>seed</source></aux>', decode('C1C2C3', 'hex'), 'xml');

INSERT INTO IT_TYPED_AUX
(ID, MAIN_ID, LABEL, PAYLOAD_XML, PAYLOAD_CLOB, PAYLOAD_BLOB, LOB_KIND)
VALUES
(12, 2, 'seed-aux-zip', '<aux><kind>zip</kind><source>seed-aux</source></aux>',
 NULL, decode('D1D2D3D4', 'hex'), 'zip');
