package io.github.yok.flexdblink.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;

/**
 * Resolves a deterministic table load order based on foreign key dependencies.
 *
 * <h2>Purpose</h2>
 *
 * <p>
 * When loading test data, parent tables must be loaded before child tables so that foreign key
 * constraints are satisfied. This class analyzes foreign key relationships via JDBC metadata and
 * returns a parent-first ordering.
 * </p>
 *
 * <h2>Algorithm</h2>
 *
 * <p>
 * This class builds a directed graph where an edge {@code parent -> child} exists if {@code child}
 * has an imported key referencing {@code parent}. It then applies Kahn's topological sort.
 * </p>
 *
 * <h2>Determinism</h2>
 *
 * <p>
 * When multiple tables are eligible at the same time (same dependency depth), they are processed in
 * alphabetical order (case-insensitive keys), ensuring deterministic output.
 * </p>
 *
 * <h2>Identifier handling (important for PostgreSQL)</h2>
 *
 * <p>
 * Some JDBC drivers match {@link DatabaseMetaData#getImportedKeys(String, String, String)} strictly
 * against catalog identifiers. For example, PostgreSQL stores unquoted identifiers in lower-case in
 * the catalog, and {@code getImportedKeys(..., "IT_TYPED_MAIN")} may return no rows.
 * </p>
 *
 * <p>
 * To avoid missing FK metadata, this class normalizes {@code schema} and {@code table} arguments
 * passed to {@code getImportedKeys} based on:
 * </p>
 *
 * <ul>
 * <li>{@link DatabaseMetaData#storesLowerCaseIdentifiers()}</li>
 * <li>{@link DatabaseMetaData#storesUpperCaseIdentifiers()}</li>
 * </ul>
 *
 * <p>
 * In addition, if {@code catalog} is non-null and the first metadata call yields no rows, this
 * class retries {@code getImportedKeys} with {@code catalog=null}. Some drivers behave differently
 * depending on the catalog argument.
 * </p>
 *
 * <h2>Rules / limitations</h2>
 *
 * <ul>
 * <li>Foreign keys referencing tables outside the provided {@code tables} list are ignored.</li>
 * <li>Self-referencing foreign keys are ignored.</li>
 * <li>Multiple FK constraints from the same child to the same parent are treated as a single
 * edge.</li>
 * <li>If the input contains duplicate table names case-insensitively, the first occurrence is used
 * and later duplicates are ignored.</li>
 * <li>If a cycle exists among the provided tables, the acyclic portion is sorted first, then the
 * cyclic tables are appended in alphabetical order.</li>
 * </ul>
 *
 * @author Yasuharu.Okawauchi
 */
@Slf4j
public final class TableDependencyResolver {

    /**
     * Prevents instantiation.
     */
    private TableDependencyResolver() {
        throw new AssertionError("TableDependencyResolver must not be instantiated.");
    }

    /**
     * Resolves a parent-first load order for the given tables.
     *
     * <h3>Input validation</h3>
     *
     * <ul>
     * <li>If {@code tables} is {@code null} or empty, this method returns an empty list.</li>
     * <li>{@code conn} must not be {@code null}.</li>
     * <li>{@code tables} must not contain {@code null} or blank table names.</li>
     * </ul>
     *
     * <h3>Output characteristics</h3>
     *
     * <ul>
     * <li>The returned list contains each unique table name once (case-insensitive uniqueness). If
     * duplicates exist in the input, only the first occurrence is preserved.</li>
     * <li>Names in the returned list are the original names from the input list (first occurrence),
     * not the normalized names used internally.</li>
     * <li>Ordering is deterministic due to alphabetical tie-breaking.</li>
     * </ul>
     *
     * <h3>How dependencies are detected</h3>
     *
     * <p>
     * For each table in the input set, this method calls
     * {@link DatabaseMetaData#getImportedKeys(String, String, String)} and reads
     * {@code PKTABLE_NAME}. If {@code child} imports keys from {@code parent}, the edge
     * {@code parent -> child} is added and {@code child}'s in-degree is incremented.
     * </p>
     *
     * @param conn JDBC connection used only for {@link DatabaseMetaData} access
     * @param catalog catalog name passed to {@code getImportedKeys}; may be {@code null}
     * @param schema schema name passed to {@code getImportedKeys}; may be {@code null}
     * @param tables list of table names to order; may be {@code null} or empty (returns empty list)
     * @return table names in load order (parent-first), deterministic
     * @throws IllegalArgumentException if {@code conn} is {@code null} or {@code tables} contains
     *         {@code null}/{@code blank}
     * @throws SQLException if metadata retrieval fails
     */
    public static List<String> resolveLoadOrder(Connection conn, String catalog, String schema,
            List<String> tables) throws SQLException {

        if (tables == null || tables.isEmpty()) {
            return new ArrayList<>();
        }

        Validate.isTrue(conn != null, "conn must not be null.");
        for (String t : tables) {
            Validate.notBlank(t, "tables must not contain null/blank names.");
        }

        if (tables.size() == 1) {
            return new ArrayList<>(tables);
        }

        // Step 1: Build case-insensitive normalization map (first occurrence wins).
        // key: lower-case table name, value: original table name from input.
        Map<String, String> normalizedMap = new LinkedHashMap<>();
        for (String t : tables) {
            String lower = t.toLowerCase(Locale.ROOT);
            if (normalizedMap.containsKey(lower)) {
                log.warn("Duplicate table name detected (case-insensitive): '{}' and '{}'. "
                        + "Using the first occurrence.", normalizedMap.get(lower), t);
                continue;
            }
            normalizedMap.put(lower, t);
        }
        Set<String> tableSetLower = normalizedMap.keySet();

        // Step 2: Collect FK edges via JDBC metadata (parent -> child).
        Map<String, Set<String>> edges = new HashMap<>();
        Map<String, Integer> inDegree = new HashMap<>();
        for (String lower : tableSetLower) {
            edges.put(lower, new HashSet<>());
            inDegree.put(lower, 0);
        }

        DatabaseMetaData meta = conn.getMetaData();
        String schemaForMeta = normalizeIdentifierForMetadata(meta, schema);

        // Iterate normalizedMap to avoid duplicate metadata calls.
        for (Map.Entry<String, String> entry : normalizedMap.entrySet()) {
            String childLower = entry.getKey();
            String childName = entry.getValue();

            String tableForMeta = normalizeIdentifierForMetadata(meta, childName);

            boolean anyRow;
            try (ResultSet rs = meta.getImportedKeys(catalog, schemaForMeta, tableForMeta)) {
                anyRow = processImportedKeys(rs, childLower, tableSetLower, edges, inDegree,
                        normalizedMap, childName);
            }

            // Retry without catalog only if the first call returned no rows.
            if (!anyRow && catalog != null) {
                try (ResultSet rs = meta.getImportedKeys(null, schemaForMeta, tableForMeta)) {
                    processImportedKeys(rs, childLower, tableSetLower, edges, inDegree,
                            normalizedMap, childName);
                }
            }
        }

        // Step 3: Kahn's topological sort with alphabetical tie-break.
        PriorityQueue<String> queue = new PriorityQueue<>();
        for (String lower : tableSetLower) {
            if (inDegree.get(lower) == 0) {
                queue.offer(lower);
            }
        }

        List<String> sorted = new ArrayList<>(tableSetLower.size());
        while (!queue.isEmpty()) {
            String current = queue.poll();
            sorted.add(current);

            for (String child : edges.get(current)) {
                int newDegree = inDegree.merge(child, -1, Integer::sum);
                if (newDegree == 0) {
                    queue.offer(child);
                }
            }
        }

        // Step 4: Handle cycles (append remaining tables alphabetically).
        if (sorted.size() < tableSetLower.size()) {
            Set<String> sortedSet = new HashSet<>(sorted);
            List<String> cyclic = tableSetLower.stream().filter(lower -> !sortedSet.contains(lower))
                    .sorted().collect(Collectors.toList());

            List<String> cyclicNames =
                    cyclic.stream().map(normalizedMap::get).collect(Collectors.toList());

            log.warn("Circular foreign key reference detected for tables: {}. "
                    + "These tables will be appended in alphabetical order.", cyclicNames);

            sorted.addAll(cyclic);
        }

        // Step 5: Map normalized keys back to original input names.
        List<String> result = sorted.stream().map(lower -> normalizedMap.getOrDefault(lower, lower))
                .collect(Collectors.toList());

        log.info("Resolved table order (parent-first): {}", result);
        return result;
    }

    /**
     * Normalizes an identifier for metadata lookups based on JDBC driver's catalog rules.
     *
     * <p>
     * Some databases store unquoted identifiers in a specific case in the system catalog. This
     * method uses {@link DatabaseMetaData#storesLowerCaseIdentifiers()} and
     * {@link DatabaseMetaData#storesUpperCaseIdentifiers()} to normalize {@code identifier}. If
     * neither flag is true, the identifier is returned unchanged.
     * </p>
     *
     * @param meta JDBC metadata
     * @param identifier identifier to normalize; may be {@code null}
     * @return normalized identifier (or {@code null} if input is {@code null})
     * @throws SQLException if metadata access fails
     */
    private static String normalizeIdentifierForMetadata(DatabaseMetaData meta, String identifier)
            throws SQLException {
        if (identifier == null) {
            return null;
        }
        if (meta.storesLowerCaseIdentifiers()) {
            return identifier.toLowerCase(Locale.ROOT);
        }
        if (meta.storesUpperCaseIdentifiers()) {
            return identifier.toUpperCase(Locale.ROOT);
        }
        return identifier;
    }

    /**
     * Processes {@link DatabaseMetaData#getImportedKeys} rows and updates the dependency graph.
     *
     * <p>
     * For each row in {@code rs}, this method reads {@code PKTABLE_NAME} and interprets it as the
     * parent table referenced by the imported key of the current child table.
     * </p>
     *
     * <p>
     * The following rules are applied:
     * </p>
     *
     * <ul>
     * <li>Rows with {@code PKTABLE_NAME == null} are ignored.</li>
     * <li>Parents not contained in {@code tableSetLower} are ignored.</li>
     * <li>Self references ({@code parent == child}) are ignored.</li>
     * <li>Duplicate edges (same parent-child pair) are ignored.</li>
     * </ul>
     *
     * @param rs imported-keys result set
     * @param childLower normalized child name (lower-case)
     * @param tableSetLower normalized table set (lower-case)
     * @param edges adjacency list ({@code parent -> children})
     * @param inDegree in-degree map ({@code child -> number of parents})
     * @param normalizedMap {@code normalized -> original} map
     * @param childName original child name for logging
     * @return {@code true} if {@code rs} had at least one row, otherwise {@code false}
     * @throws SQLException if result set access fails
     */
    private static boolean processImportedKeys(ResultSet rs, String childLower,
            Set<String> tableSetLower, Map<String, Set<String>> edges,
            Map<String, Integer> inDegree, Map<String, String> normalizedMap, String childName)
            throws SQLException {

        boolean anyRow = false;

        while (rs.next()) {
            anyRow = true;

            String pkTable = rs.getString("PKTABLE_NAME");
            if (pkTable == null) {
                continue;
            }
            String parentLower = pkTable.toLowerCase(Locale.ROOT);

            if (!tableSetLower.contains(parentLower)) {
                continue;
            }
            if (parentLower.equals(childLower)) {
                continue;
            }

            if (edges.get(parentLower).add(childLower)) {
                inDegree.merge(childLower, 1, Integer::sum);
                log.debug("FK dependency detected: parent='{}' -> child='{}'",
                        normalizedMap.getOrDefault(parentLower, pkTable),
                        normalizedMap.getOrDefault(childLower, childName));
            }
        }

        return anyRow;
    }
}
