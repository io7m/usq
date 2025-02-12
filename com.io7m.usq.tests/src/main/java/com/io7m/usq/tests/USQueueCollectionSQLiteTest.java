/*
 * Copyright © 2024 Mark Raynsford <code@io7m.com> https://www.io7m.com
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR
 * IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */


package com.io7m.usq.tests;

import com.io7m.usq.api.USQException;
import com.io7m.usq.api.USQueueCollectionConfiguration;
import com.io7m.usq.api.USQueueCollectionType;
import com.io7m.usq.sqlite.USQSQLiteProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;

import java.nio.file.Path;
import java.time.Duration;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public final class USQueueCollectionSQLiteTest
  extends USQueueCollectionContract
{
  private static final Logger LOG =
    LoggerFactory.getLogger(USQueueCollectionSQLiteTest.class);

  public static final Consumer<String> IGNORE = s -> {

  };

  @Override
  protected Logger logger()
  {
    return LOG;
  }

  @Override
  protected USQueueCollectionType createCollection(
    final Path file)
    throws Exception
  {
    final var provider = new USQSQLiteProvider();

    final var configuration =
      USQueueCollectionConfiguration.builder()
        .setFile(file)
        .setMessageExpirationFrequency(Duration.ofSeconds(60L))
        .setMessageExpirationInitialDelay(Duration.ofSeconds(60L))
        .setStartupEventReceiver(s -> LOG.debug("{}", s))
        .build();

    return provider.open(configuration);
  }

  @Override
  protected USQueueCollectionType createCollectionWithImmediateExpiration(
    final Path file)
    throws Exception
  {
    final var provider = new USQSQLiteProvider();

    final var configuration =
      USQueueCollectionConfiguration.builder()
        .setFile(file)
        .setMessageExpirationFrequency(Duration.ofMillis(100L))
        .setMessageExpirationInitialDelay(Duration.ofMillis(1L))
        .setStartupEventReceiver(s -> LOG.debug("{}", s))
        .build();

    return provider.open(configuration);
  }

  @Test
  public void testDatabaseTooNew(
    final @TempDir Path directory)
    throws Exception
  {
    final var provider =
      new USQSQLiteProvider();
    final var file =
      directory.resolve("toonew.db");

    final var configuration =
      USQueueCollectionConfiguration.builder()
        .setFile(file)
        .setMessageExpirationFrequency(Duration.ofSeconds(60L))
        .setMessageExpirationInitialDelay(Duration.ofSeconds(60L))
        .setStartupEventReceiver(s -> LOG.debug("{}", s))
        .build();

    try (var db = provider.open(configuration)) {

    }

    {
      final var url = new StringBuilder(128);
      url.append("jdbc:sqlite:");
      url.append(file);
      final var config = new SQLiteConfig();
      final var dataSource = new SQLiteDataSource(config);
      dataSource.setUrl(url.toString());
      try (var conn = dataSource.getConnection()) {
        conn.setAutoCommit(false);
        try (var st = conn.prepareStatement(
          "UPDATE schema_version SET version_number = 200000")) {
          st.executeUpdate();
        }
        conn.commit();
      }
    }

    final var ex =
      assertThrows(USQException.class, () -> provider.open(configuration));

    assertEquals("error-database-upgrade", ex.errorCode());
  }

  @Test
  public void testDatabaseTruncated(
    final @TempDir Path directory)
    throws Exception
  {
    final var provider =
      new USQSQLiteProvider();
    final var file =
      directory.resolve("toonew.db");

    final var configuration =
      USQueueCollectionConfiguration.builder()
        .setFile(file)
        .setMessageExpirationFrequency(Duration.ofSeconds(60L))
        .setMessageExpirationInitialDelay(Duration.ofSeconds(60L))
        .setStartupEventReceiver(s -> LOG.debug("{}", s))
        .build();

    try (var db = provider.open(configuration)) {

    }

    {
      final var url = new StringBuilder(128);
      url.append("jdbc:sqlite:");
      url.append(file);
      final var config = new SQLiteConfig();
      final var dataSource = new SQLiteDataSource(config);
      dataSource.setUrl(url.toString());
      try (var conn = dataSource.getConnection()) {
        conn.setAutoCommit(false);
        try (var st = conn.prepareStatement(
          "DELETE FROM schema_version")) {
          st.executeUpdate();
        }
        conn.commit();
      }
    }

    final var ex =
      assertThrows(USQException.class, () -> provider.open(configuration));

    assertEquals("error-database-upgrade", ex.errorCode());
  }

  @Test
  public void testDatabaseWrongApplication(
    final @TempDir Path directory)
    throws Exception
  {
    final var provider =
      new USQSQLiteProvider();
    final var file =
      directory.resolve("toonew.db");

    final var configuration =
      USQueueCollectionConfiguration.builder()
        .setFile(file)
        .setMessageExpirationFrequency(Duration.ofSeconds(60L))
        .setMessageExpirationInitialDelay(Duration.ofSeconds(60L))
        .setStartupEventReceiver(s -> LOG.debug("{}", s))
        .build();

    try (var db = provider.open(configuration)) {

    }

    {
      final var url = new StringBuilder(128);
      url.append("jdbc:sqlite:");
      url.append(file);
      final var config = new SQLiteConfig();
      final var dataSource = new SQLiteDataSource(config);
      dataSource.setUrl(url.toString());
      try (var conn = dataSource.getConnection()) {
        conn.setAutoCommit(false);
        try (var st = conn.prepareStatement(
          "UPDATE schema_version SET version_application_id = 'wrong'")) {
          st.executeUpdate();
        }
        conn.commit();
      }
    }

    final var ex =
      assertThrows(USQException.class, () -> provider.open(configuration));

    assertEquals("error-database-upgrade", ex.errorCode());
  }
}
