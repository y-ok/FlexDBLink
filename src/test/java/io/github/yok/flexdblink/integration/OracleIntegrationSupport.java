package io.github.yok.flexdblink.integration;

import io.github.yok.flexdblink.config.ConnectionConfig;
import io.github.yok.flexdblink.config.CsvDateTimeFormatProperties;
import io.github.yok.flexdblink.config.DataTypeFactoryMode;
import io.github.yok.flexdblink.config.DbUnitConfig;
import io.github.yok.flexdblink.config.DumpConfig;
import io.github.yok.flexdblink.config.FilePatternConfig;
import io.github.yok.flexdblink.config.PathsConfig;
import io.github.yok.flexdblink.core.DataDumper;
import io.github.yok.flexdblink.core.DataLoader;
import io.github.yok.flexdblink.db.DbDialectHandlerFactory;
import io.github.yok.flexdblink.db.DbUnitConfigFactory;
import io.github.yok.flexdblink.util.DateTimeFormatUtil;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.flywaydb.core.Flyway;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.oracle.OracleContainer;

/**
 * Oracle コンテナに対するインテグレーション試験支援ユーティリティです。
 *
 * <p>
 * 本クラスは、Testcontainers で起動した Oracle に対して Flyway を実行し、FlexDBLink の DataLoader / DataDumper
 * を実データに近い条件で動作させるための「試験用ランタイム」を構築します。
 * </p>
 *
 * <h2>前提のテストリソース構成</h2>
 *
 * <pre>
 * src/test/resources
 * ├── db/migration/oracle
 * │   ├── V1__create_oracle_integration_test_tables.sql
 * │   └── V2__seed_integration_test_data.sql
 * └── integration/oracle/load
 *     └── pre
 *         └── db1
 *             ├── files
 *             │   └── (LOB ファイル群)
 *             ├── IT_TYPED_MAIN.csv
 *             ├── IT_TYPED_AUX.csv
 *             └── table-ordering.txt
 * </pre>
 *
 * <h2>責務</h2>
 * <ul>
 * <li>DB準備：Oracle コンテナ起動と Flyway clean/migrate</li>
 * <li>実行準備：Paths/Connection/DbUnit/Dump/Pattern/DateTime/DialectFactory を組み立て</li>
 * <li>実行：load/dump と CSV 参照ユーティリティの提供</li>
 * </ul>
 */
final class OracleIntegrationSupport {

    private OracleIntegrationSupport() {}

    /**
     * Oracle コンテナを起動し、Flyway を実行して DB を試験可能な状態に初期化します。
     *
     * <p>
     * 既存データが残らないことを担保するため、Flyway の clean → migrate を実行します。
     * </p>
     *
     * @param container Oracle コンテナ
     */
    static void prepareDatabase(OracleContainer container) {
        ensureContainerRunning(container);
        migrate(container);
    }

    /**
     * インテグレーション試験用ランタイムを構築します。
     *
     * <p>
     * {@code copyLoadFixtures} が {@code true} の場合、以下のリソースを {@code dataPath/load} にコピーします。
     * </p>
     *
     * <pre>
     * src/test/resources/integration/oracle/load/** → ${dataPath}/load/**
     * src/test/resources/integration/common/load/** → ${dataPath}/load/**
     * </pre>
     *
     * @param container Oracle コンテナ
     * @param dataPath 試験データ用のルートパス（テンポラリを想定）
     * @param copyLoadFixtures ロード用フィクスチャをコピーする場合 true
     * @return ランタイム
     * @throws IOException ファイルI/Oに失敗した場合
     */
    static Runtime prepareRuntime(OracleContainer container, Path dataPath,
            boolean copyLoadFixtures) throws IOException {
        Files.createDirectories(dataPath);
        if (copyLoadFixtures) {
            copyLoadFixtures(dataPath);
        }

        PathsConfig pathsConfig = pathsConfig(dataPath);
        ConnectionConfig connectionConfig = connectionConfig(container);
        DbUnitConfig dbUnitConfig = dbUnitConfig();
        DumpConfig dumpConfig = dumpConfig();
        dumpConfig.setExcludeTables(List.of("FLYWAY_SCHEMA_HISTORY"));
        FilePatternConfig filePatternConfig = filePatternConfig();
        DateTimeFormatUtil dateTimeUtil = dateTimeUtil();
        DbDialectHandlerFactory factory =
                dialectFactory(dbUnitConfig, dumpConfig, pathsConfig, dateTimeUtil);

        return new Runtime(dataPath, connectionConfig, dbUnitConfig, dumpConfig, pathsConfig,
                filePatternConfig, factory);
    }

    /**
     * DataLoader を使って指定シナリオを実行します。
     *
     * <p>
     * 実行対象の接続IDは {@code db1} 固定です。
     * </p>
     *
     * @param runtime ランタイム
     * @param scenario シナリオ名（例：pre）
     */
    static void executeLoad(Runtime runtime, String scenario) {
        DataLoader loader = runtime.newLoader();
        loader.execute(scenario, List.of("db1"));
    }

    /**
     * DataDumper を使って指定シナリオを実行し、出力先ディレクトリを返します。
     *
     * <p>
     * 出力先は {@code ${dataPath}/dump/${scenario}/db1} です。
     * </p>
     *
     * @param runtime ランタイム
     * @param scenario シナリオ名
     * @return 出力先ディレクトリ
     */
    static Path executeDump(Runtime runtime, String scenario) {
        DataDumper dumper = runtime.newDumper();
        dumper.execute(scenario, List.of("db1"));
        return runtime.dataPath().resolve("dump").resolve(scenario).resolve("db1");
    }

    /**
     * Oracle コンテナへ JDBC 接続を開きます。
     *
     * @param container Oracle コンテナ
     * @return JDBC 接続
     * @throws SQLException JDBC エラー
     */
    static Connection openConnection(OracleContainer container) throws SQLException {
        return DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(),
                container.getPassword());
    }

    /**
     * 指定 CSV から {@code ID} 列で 1 行を取得します。
     *
     * @param csvPath CSV パス
     * @param id ID 値
     * @return 行データ（ヘッダ→値）
     * @throws IOException CSV 読み取り失敗
     */
    static Map<String, String> readCsvRowById(Path csvPath, String id) throws IOException {
        Map<String, Map<String, String>> rows = readCsvById(csvPath, "ID");
        Map<String, String> row = rows.get(id);
        if (row == null) {
            throw new IllegalStateException("Row not found: ID=" + id + " in " + csvPath);
        }
        return row;
    }

    /**
     * 指定 CSV を {@code idColumn} 列でマップ化して取得します。
     *
     * <p>
     * CSV の先頭行をヘッダとして扱い、ヘッダ順序を保持するため内部は {@link LinkedHashMap} を使用します。
     * </p>
     *
     * @param csvPath CSV パス
     * @param idColumn ID 列名
     * @return idColumn の値 → (ヘッダ → 値)
     * @throws IOException CSV 読み取り失敗
     */
    static Map<String, Map<String, String>> readCsvById(Path csvPath, String idColumn)
            throws IOException {
        Map<String, Map<String, String>> rows = new LinkedHashMap<>();
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true)
                .setIgnoreSurroundingSpaces(false).setTrim(false).get();

        try (CSVParser parser = CSVParser.parse(csvPath, StandardCharsets.UTF_8, format)) {
            for (CSVRecord record : parser) {
                Map<String, String> row = new LinkedHashMap<>();
                for (String header : parser.getHeaderMap().keySet()) {
                    row.put(header, record.get(header));
                }
                rows.put(record.get(idColumn), row);
            }
        }
        return rows;
    }

    /**
     * 16進文字列（HEX）をバイト配列へデコードします。
     *
     * @param value 16進文字列
     * @return バイト配列（空または null の場合は空配列）
     */
    static byte[] decodeHex(String value) {
        if (value == null || value.isEmpty()) {
            return new byte[0];
        }
        try {
            return Hex.decodeHex(value);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid hex value: " + value, e);
        }
    }

    /**
     * トリムして空なら null を返します。
     *
     * @param value 入力値
     * @return null またはトリム済み文字列
     */
    static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        return trimmed;
    }

    /**
     * トリムして null を空文字に変換します。
     *
     * @param value 入力値
     * @return 空文字またはトリム済み文字列
     */
    static String trimToEmpty(String value) {
        if (value == null) {
            return "";
        }
        return value.trim();
    }

    /**
     * Flyway を用いて Oracle スキーマを初期化します。
     *
     * <p>
     * マイグレーションの location は {@code classpath:db/migration/oracle} 固定です。
     * </p>
     *
     * @param container Oracle コンテナ
     */
    static void migrate(OracleContainer container) {
        Flyway flyway = Flyway
                .configure().cleanDisabled(false).dataSource(container.getJdbcUrl(),
                        container.getUsername(), container.getPassword())
                .locations("classpath:db/migration/oracle").load();
        flyway.clean();
        flyway.migrate();
    }

    /**
     * テストリソースのロード用フィクスチャを {@code dataPath/load} にコピーします。
     *
     * <p>
     * 新しいリソース構成に合わせ、コピー元は以下です。
     * </p>
     *
     * <pre>
     * src/test/resources/integration/oracle/load/** → ${dataPath}/load/**
     * src/test/resources/integration/common/load/** → ${dataPath}/load/**
     * </pre>
     *
     * @param dataPath 試験データ用ルートパス
     * @throws IOException コピー失敗
     */
    static void copyLoadFixtures(Path dataPath) throws IOException {
        Path src = Path.of("src", "test", "resources", "integration", "oracle", "load");
        Path commonSrc = Path.of("src", "test", "resources", "integration", "common", "load");
        Path dst = dataPath.resolve("load");
        Files.createDirectories(dst);
        copyTree(src, dst);
        copyTree(commonSrc, dst);
    }

    /**
     * Copies all files from source tree into destination tree.
     *
     * @param src source root
     * @param dst destination root
     * @throws IOException when file copy fails
     */
    private static void copyTree(Path src, Path dst) throws IOException {
        try (var stream = Files.walk(src)) {
            stream.forEach(path -> {
                try {
                    Path rel = src.relativize(path);
                    Path target = dst.resolve(rel);
                    if (Files.isDirectory(path)) {
                        Files.createDirectories(target);
                    } else {
                        Files.createDirectories(target.getParent());
                        Files.copy(path, target, StandardCopyOption.REPLACE_EXISTING);
                    }
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            });
        }
    }

    /**
     * PathsConfig を生成します。
     *
     * @param dataPath 試験データ用ルートパス
     * @return PathsConfig
     */
    static PathsConfig pathsConfig(Path dataPath) {
        PathsConfig pathsConfig = new PathsConfig();
        pathsConfig.setDataPath(dataPath.toAbsolutePath().toString());
        return pathsConfig;
    }

    /**
     * ConnectionConfig を生成します。
     *
     * <p>
     * 接続IDは {@code db1} 固定です。
     * </p>
     *
     * @param container Oracle コンテナ
     * @return ConnectionConfig
     */
    static ConnectionConfig connectionConfig(OracleContainer container) {
        ConnectionConfig connectionConfig = new ConnectionConfig();
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setUrl(container.getJdbcUrl());
        entry.setUser(container.getUsername());
        entry.setPassword(container.getPassword());
        entry.setDriverClass("oracle.jdbc.OracleDriver");
        connectionConfig.setConnections(List.of(entry));
        return connectionConfig;
    }

    /**
     * DbUnitConfig を生成します。
     *
     * @return DbUnitConfig
     */
    static DbUnitConfig dbUnitConfig() {
        DbUnitConfig cfg = new DbUnitConfig();
        cfg.setDataTypeFactoryMode(DataTypeFactoryMode.ORACLE);
        cfg.setPreDirName("pre");
        return cfg;
    }

    /**
     * DumpConfig を生成します。
     *
     * @return DumpConfig
     */
    static DumpConfig dumpConfig() {
        return new DumpConfig();
    }

    /**
     * CSV 日時フォーマット定義に基づいて DateTimeFormatUtil を生成します。
     *
     * @return DateTimeFormatUtil
     */
    static DateTimeFormatUtil dateTimeUtil() {
        CsvDateTimeFormatProperties props = new CsvDateTimeFormatProperties();
        props.setDate("yyyy-MM-dd");
        props.setTime("HH:mm:ss");
        props.setDateTime("yyyy-MM-dd HH:mm:ss");
        props.setDateTimeWithMillis("yyyy-MM-dd HH:mm:ss.SSS");
        return new DateTimeFormatUtil(props);
    }

    /**
     * DbDialectHandlerFactory を生成します。
     *
     * @param dbUnitConfig DbUnitConfig
     * @param dumpConfig DumpConfig
     * @param pathsConfig PathsConfig
     * @param dateTimeUtil DateTimeFormatUtil
     * @return DbDialectHandlerFactory
     */
    static DbDialectHandlerFactory dialectFactory(DbUnitConfig dbUnitConfig, DumpConfig dumpConfig,
            PathsConfig pathsConfig, DateTimeFormatUtil dateTimeUtil) {
        return new DbDialectHandlerFactory(dbUnitConfig, dumpConfig, pathsConfig, dateTimeUtil,
                new DbUnitConfigFactory());
    }

    /**
     * FilePatternConfig を生成します。
     *
     * <p>
     * LOB ファイル出力名の規定パターンを定義します。
     * </p>
     *
     * @return FilePatternConfig
     */
    static FilePatternConfig filePatternConfig() {
        FilePatternConfig filePatternConfig = new FilePatternConfig();
        Map<String, Map<String, String>> map = new HashMap<>();

        Map<String, String> main = new LinkedHashMap<>();
        main.put("CLOB_COL", "main_{LOB_KIND}_{ID}.txt");
        main.put("NCLOB_COL", "main_n_{LOB_KIND}_{ID}.txt");
        main.put("BLOB_COL", "main_{LOB_KIND}_{ID}.bin");
        map.put("IT_TYPED_MAIN", main);

        Map<String, String> aux = new LinkedHashMap<>();
        aux.put("PAYLOAD_CLOB", "aux_{LOB_KIND}_{ID}.txt");
        aux.put("PAYLOAD_BLOB", "aux_{LOB_KIND}_{ID}.bin");
        map.put("IT_TYPED_AUX", aux);

        filePatternConfig.setFilePatterns(map);
        return filePatternConfig;
    }

    /**
     * Oracle コンテナを起動します。
     *
     * @param container Oracle コンテナ
     */
    static void ensureContainerRunning(OracleContainer container) {
        try {
            container.start();
        } catch (ContainerLaunchException e) {
            throw new IllegalStateException(
                    "Oracle container must be available for integration tests", e);
        }
    }

    /**
     * インテグレーション試験の実行に必要な設定一式を保持するランタイムです。
     *
     * <p>
     * DataLoader/DataDumper を生成する際の依存（PathsConfig/ConnectionConfig 等）をまとめて保持します。
     * </p>
     */
    static final class Runtime {

        private final Path dataPath;

        private final ConnectionConfig connectionConfig;

        private final DbUnitConfig dbUnitConfig;

        private final DumpConfig dumpConfig;

        private final PathsConfig pathsConfig;

        private final FilePatternConfig filePatternConfig;

        private final DbDialectHandlerFactory dialectFactory;

        Runtime(Path dataPath, ConnectionConfig connectionConfig, DbUnitConfig dbUnitConfig,
                DumpConfig dumpConfig, PathsConfig pathsConfig, FilePatternConfig filePatternConfig,
                DbDialectHandlerFactory dialectFactory) {
            this.dataPath = dataPath;
            this.connectionConfig = connectionConfig;
            this.dbUnitConfig = dbUnitConfig;
            this.dumpConfig = dumpConfig;
            this.pathsConfig = pathsConfig;
            this.filePatternConfig = filePatternConfig;
            this.dialectFactory = dialectFactory;
        }

        Path dataPath() {
            return dataPath;
        }

        /**
         * DataLoader を生成します（Oracle 用）。
         *
         * @return DataLoader
         */
        DataLoader newLoader() {
            return new DataLoader(pathsConfig, connectionConfig, dialectFactory::create,
                    dbUnitConfig, dumpConfig);
        }

        /**
         * DataDumper を生成します（Oracle 用）。
         *
         * @return DataDumper
         */
        DataDumper newDumper() {
            return new DataDumper(pathsConfig, connectionConfig, filePatternConfig, dumpConfig,
                    dialectFactory::create);
        }
    }
}
