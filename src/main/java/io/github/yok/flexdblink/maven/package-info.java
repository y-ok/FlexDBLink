/**
 * Maven and JUnit integration package for FlexDBLink.
 *
 * <p>
 * Provides JUnit 5 extensions and annotations to load test data automatically before tests run, as
 * well as utilities to resolve test resources and manage DataSource/transaction participation.
 * </p>
 *
 * <p>
 * Typical usage is via {@code @LoadData} and {@code LoadDataExtension}, which locate input
 * resources, execute the load workflow, and coordinate transactions for reliable test isolation.
 * </p>
 */
package io.github.yok.flexdblink.maven;
