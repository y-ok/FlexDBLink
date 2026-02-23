/**
 * Core load/dump workflow package.
 *
 * <p>
 * Orchestrates dumping data from a database to files and loading data from files into a database.
 * Includes CSV export, LOB file export, and scenario handling such as duplicate detection and
 * deletion.
 * </p>
 *
 * <p>
 * Database-specific differences (type conversion, SQL dialects, etc.) are delegated to handlers in
 * {@code db}.
 * </p>
 */
package io.github.yok.flexdblink.core;
