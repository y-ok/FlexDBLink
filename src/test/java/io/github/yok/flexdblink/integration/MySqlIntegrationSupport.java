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
import io.github.yok.flexdblink.util.OracleDateTimeFormatUtil;
import java.io.IOException;
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
import org.flywaydb.core.Flyway;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.MySQLContainer;

/**
 * MySQL インテグレーション試験の支援ユーティリティです。
 *
 * <p>
 * Testcontainers + Flyway で MySQL を初期化し、FlexDBLink（DataLoader/DataDumper）を実運用に近い構成で実行します。
 * </p>
 *
 * <ul>
 * <li>DB準備：Flyway clean/migrate（classpath:db/migration/mysql）</li>
 * <li>実行準備：Paths/Connection/DbUnit/Dump/Pattern/DateTime/DialectFactory を組み立て</li>
 * <li>実行：load/dump を呼び出し</li>
 * </ul>
 */
final class MySqlIntegrationSupport {

    private MySqlIntegrationSupport() {}

    /**
     * MySQL コンテナを起動し、Flyway によりテスト用スキーマを再作成します。
     *
     * <p>
     * 本メソッドは冪等ではありません。毎回 clean/migrate を行い、テスト開始時に DB を初期状態へ戻します。
     * </p>
     *
     * @param container MySQL コンテナ
     */
    static void prepareDatabase(MySQLContainer<?> container) {
        ensureContainerRunning(container);
        migrate(container);
    }

    /**
     * インテグレーション試験の実行に必要なランタイム一式を組み立てます。
     *
     * <p>
     * {@code dataPath} 配下に以下の構造を作成して利用します。
     * </p>
     *
     * <ul>
     * <li>load/{scenario}/{dbId}/... : DataLoader 入力</li>
     * <li>dump/{scenario}/{dbId}/... : DataDumper 出力</li>
     * </ul>
     *
     * <p>
     * {@code copyLoadFixtures=true} の場合、 {@code src/test/resources/integration/mysql/load} と
     * {@code src/test/resources/integration/common/load} を {@code dataPath/load} へコピーします。
     * </p>
     *
     * @param container MySQL コンテナ
     * @param dataPath テストで使用する作業ディレクトリ
     * @param copyLoadFixtures load 配下のフィクスチャを作業ディレクトリへコピーする場合 true
     * @return 実行用ランタイム
     * @throws IOException ファイル操作に失敗した場合
     */
    static Runtime prepareRuntime(MySQLContainer<?> container, Path dataPath,
            boolean copyLoadFixtures) throws IOException {
        Files.createDirectories(dataPath);
        if (copyLoadFixtures) {
            copyLoadFixtures(dataPath);
        }

        PathsConfig pathsConfig = pathsConfig(dataPath);
        ConnectionConfig connectionConfig = connectionConfig(container);
        DbUnitConfig dbUnitConfig = dbUnitConfig();
        DumpConfig dumpConfig = dumpConfig();

        // Flyway の履歴テーブルはダンプ対象から除外する
        dumpConfig.setExcludeTables(List.of("flyway_schema_history"));

        FilePatternConfig filePatternConfig = filePatternConfig();
        OracleDateTimeFormatUtil dateTimeUtil = dateTimeUtil();
        DbDialectHandlerFactory factory =
                dialectFactory(dbUnitConfig, dumpConfig, pathsConfig, dateTimeUtil);

        return new Runtime(dataPath, connectionConfig, dbUnitConfig, dumpConfig, pathsConfig,
                filePatternConfig, factory);
    }

    /**
     * 指定シナリオの load を実行します。
     *
     * @param runtime ランタイム
     * @param scenario シナリオ名（例：pre）
     */
    static void executeLoad(Runtime runtime, String scenario) {
        DataLoader loader = runtime.newLoader();
        loader.execute(scenario, List.of("db1"));
    }

    /**
     * 指定シナリオの dump を実行し、出力先ディレクトリを返します。
     *
     * @param runtime ランタイム
     * @param scenario シナリオ名
     * @return dump/{scenario}/db1 ディレクトリ
     */
    static Path executeDump(Runtime runtime, String scenario) {
        DataDumper dumper = runtime.newDumper();
        dumper.execute(scenario, List.of("db1"));
        return runtime.dataPath().resolve("dump").resolve(scenario).resolve("db1");
    }

    /**
     * MySQL コンテナへ JDBC 接続します。
     *
     * @param container MySQL コンテナ
     * @return JDBC コネクション
     * @throws SQLException 接続に失敗した場合
     */
    static Connection openConnection(MySQLContainer<?> container) throws SQLException {
        return DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(),
                container.getPassword());
    }

    /**
     * Flyway によりテスト用スキーマを再作成します。
     *
     * <p>
     * {@code classpath:db/migration/mysql} を対象に clean/migrate を行います。
     * </p>
     *
     * @param container MySQL コンテナ
     */
    static void migrate(MySQLContainer<?> container) {
        Flyway flyway = Flyway
                .configure().cleanDisabled(false).dataSource(container.getJdbcUrl(),
                        container.getUsername(), container.getPassword())
                .locations("classpath:db/migration/mysql").load();
        flyway.clean();
        flyway.migrate();
    }

    /**
     * MySQL 用の load フィクスチャを作業ディレクトリへコピーします。
     *
     * <p>
     * コピー元： {@code src/test/resources/integration/mysql/load}<br>
     * コピー元： {@code src/test/resources/integration/common/load}<br>
     * コピー先： {@code dataPath/load}
     * </p>
     *
     * @param dataPath 作業ディレクトリ
     * @throws IOException コピーに失敗した場合
     */
    static void copyLoadFixtures(Path dataPath) throws IOException {
        Path src = Path.of("src", "test", "resources", "integration", "mysql", "load");
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
     * @param dataPath 作業ディレクトリ
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
     * dbId は {@code db1} 固定で構成します。
     * </p>
     *
     * @param container MySQL コンテナ
     * @return ConnectionConfig
     */
    static ConnectionConfig connectionConfig(MySQLContainer<?> container) {
        ConnectionConfig connectionConfig = new ConnectionConfig();
        ConnectionConfig.Entry entry = new ConnectionConfig.Entry();
        entry.setId("db1");
        entry.setUrl(container.getJdbcUrl());
        entry.setUser(container.getUsername());
        entry.setPassword(container.getPassword());
        entry.setDriverClass("com.mysql.cj.jdbc.Driver");
        connectionConfig.setConnections(List.of(entry));
        return connectionConfig;
    }

    /**
     * DbUnitConfig を生成します。
     *
     * <p>
     * MySQL モードとして DBUnit の DataTypeFactory を MySQL 用へ切り替えます。
     * </p>
     *
     * @return DbUnitConfig
     */
    static DbUnitConfig dbUnitConfig() {
        DbUnitConfig cfg = new DbUnitConfig();
        cfg.setDataTypeFactoryMode(DataTypeFactoryMode.MYSQL);
        cfg.setPreDirName("pre");
        cfg.setLobDirName("files");
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
     * 日時フォーマット（CSV⇔DB 変換）ユーティリティを生成します。
     *
     * <p>
     * Oracle 用クラス名ですが、CSV の日時正規化ロジックとして共通利用します。
     * </p>
     *
     * @return OracleDateTimeFormatUtil
     */
    static OracleDateTimeFormatUtil dateTimeUtil() {
        CsvDateTimeFormatProperties props = new CsvDateTimeFormatProperties();
        props.setDate("yyyy-MM-dd");
        props.setTime("HH:mm:ss");
        props.setDateTime("yyyy-MM-dd HH:mm:ss");
        props.setDateTimeWithMillis("yyyy-MM-dd HH:mm:ss.SSS");
        return new OracleDateTimeFormatUtil(props);
    }

    /**
     * DbDialectHandlerFactory を生成します。
     *
     * @param dbUnitConfig DbUnitConfig
     * @param dumpConfig DumpConfig
     * @param pathsConfig PathsConfig
     * @param dateTimeUtil 日時フォーマットユーティリティ
     * @return DbDialectHandlerFactory
     */
    static DbDialectHandlerFactory dialectFactory(DbUnitConfig dbUnitConfig, DumpConfig dumpConfig,
            PathsConfig pathsConfig, OracleDateTimeFormatUtil dateTimeUtil) {
        return new DbDialectHandlerFactory(dbUnitConfig, dumpConfig, pathsConfig, dateTimeUtil,
                new DbUnitConfigFactory());
    }

    /**
     * LOB ファイル命名パターン設定を生成します。
     *
     * <p>
     * DataDumper が LOB を {@code file:xxxx} 参照へ置換し、files ディレクトリへ出力する際のファイル名を決定します。
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
     * MySQL コンテナの起動を保証します。
     *
     * @param container MySQL コンテナ
     */
    static void ensureContainerRunning(MySQLContainer<?> container) {
        if (container.isRunning()) {
            return;
        }
        try {
            container.start();
        } catch (ContainerLaunchException e) {
            throw new IllegalStateException(
                    "MySQL container must be available for integration tests", e);
        }
    }

    /**
     * インテグレーション試験実行に必要な設定一式を保持します。
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

        /**
         * 作業ディレクトリを返します。
         *
         * @return 作業ディレクトリ
         */
        Path dataPath() {
            return dataPath;
        }

        /**
         * DataLoader を生成します。
         *
         * <p>
         * MySQL のスキーマは {@code testdb} 固定として扱います。
         * </p>
         *
         * @return DataLoader
         */
        DataLoader newLoader() {
            return new DataLoader(pathsConfig, connectionConfig, entry -> "testdb",
                    dialectFactory::create, dbUnitConfig, dumpConfig);
        }

        /**
         * DataDumper を生成します。
         *
         * <p>
         * MySQL のスキーマは {@code testdb} 固定として扱います。
         * </p>
         *
         * @return DataDumper
         */
        DataDumper newDumper() {
            return new DataDumper(pathsConfig, connectionConfig, filePatternConfig, dumpConfig,
                    entry -> "testdb", dialectFactory::create);
        }
    }
}
