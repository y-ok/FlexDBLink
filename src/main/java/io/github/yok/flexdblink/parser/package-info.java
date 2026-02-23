/**
 * Data parser package for FlexDBLink.
 *
 * <p>
 * Defines a common {@code DataParser} abstraction and concrete parsers for supported formats such
 * as CSV, JSON, YAML, and XML. Parsers read input files into an internal representation used by the
 * load workflow.
 * </p>
 *
 * <p>
 * Format detection and parser instantiation are handled by {@code DataLoaderFactory} and
 * {@code DataFormat}.
 * </p>
 */
package io.github.yok.flexdblink.parser;
