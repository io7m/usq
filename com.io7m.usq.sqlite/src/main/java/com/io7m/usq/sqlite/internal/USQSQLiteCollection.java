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


package com.io7m.usq.sqlite.internal;

import com.io7m.anethum.api.ParsingException;
import com.io7m.jmulticlose.core.CloseableCollection;
import com.io7m.jmulticlose.core.CloseableCollectionType;
import com.io7m.trasco.api.TrArguments;
import com.io7m.trasco.api.TrEventExecutingSQL;
import com.io7m.trasco.api.TrEventType;
import com.io7m.trasco.api.TrEventUpgrading;
import com.io7m.trasco.api.TrException;
import com.io7m.trasco.api.TrExecutorConfiguration;
import com.io7m.trasco.api.TrSchemaRevisionSet;
import com.io7m.trasco.vanilla.TrExecutors;
import com.io7m.trasco.vanilla.TrSchemaRevisionSetParsers;
import com.io7m.usq.api.USQEventMessageExpirationFailed;
import com.io7m.usq.api.USQEventMessageExpired;
import com.io7m.usq.api.USQEventType;
import com.io7m.usq.api.USQException;
import com.io7m.usq.api.USQueueCollectionConfigurationType;
import com.io7m.usq.api.USQueueCollectionType;
import com.io7m.usq.api.USQueueType;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;
import org.sqlite.SQLiteErrorCode;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static com.io7m.trasco.api.TrExecutorUpgrade.PERFORM_UPGRADES;
import static java.math.BigInteger.valueOf;

/**
 * An SQLite-based queue collection.
 */

public final class USQSQLiteCollection
  implements USQueueCollectionType
{
  private static final String QUEUE_LIST = """
    SELECT q_name FROM queues ORDER BY q_name
    """.trim();

  private static final String MESSAGE_DELETE_EXPIRED = """
    DELETE FROM messages
      WHERE messages.msg_expires_on < ?
        RETURNING messages.msg_id
    """.trim();

  private static final String DATABASE_APPLICATION_ID =
    "com.io7m.usq";
  private static final int APPLICATION_ID =
    0x5553_5121;

  private final SQLiteDataSource dataSource;
  private final AtomicBoolean closed;
  private final SubmissionPublisher<USQEventType> events;
  private final CloseableCollectionType<USQException> resources;
  private final Semaphore connectionSemaphore;
  private final HashMap<String, USQSQLiteQueue> queuesOpen;
  private final ReentrantLock queuesOpenLock;

  private USQSQLiteCollection(
    final SQLiteDataSource inDataSource)
  {
    this.dataSource =
      Objects.requireNonNull(inDataSource, "dataSource");
    this.closed =
      new AtomicBoolean(false);
    this.connectionSemaphore =
      new Semaphore(1);

    this.queuesOpenLock =
      new ReentrantLock();
    this.queuesOpen =
      new HashMap<>();

    this.resources =
      CloseableCollection.create(() -> {
        return new USQException(
          "One or more resources could not be closed.",
          "error-resource-closing",
          Map.of(),
          Optional.empty()
        );
      });
    this.events =
      this.resources.add(new SubmissionPublisher<>());
  }

  private static void schemaVersionSet(
    final BigInteger version,
    final Connection connection)
    throws SQLException
  {
    final String statementText;
    if (Objects.equals(version, BigInteger.ZERO)) {
      statementText = "insert into schema_version (version_application_id, version_number) values (?, ?)";
      try (var statement =
             connection.prepareStatement(statementText)) {
        statement.setString(1, DATABASE_APPLICATION_ID);
        statement.setLong(2, version.longValueExact());
        statement.execute();
      }
    } else {
      statementText = "update schema_version set version_number = ?";
      try (var statement =
             connection.prepareStatement(statementText)) {
        statement.setLong(1, version.longValueExact());
        statement.execute();
      }
    }
  }

  private static Optional<BigInteger> schemaVersionGet(
    final Connection connection)
    throws SQLException
  {
    Objects.requireNonNull(connection, "connection");

    try {
      final var statementText =
        "SELECT version_application_id, version_number FROM schema_version";

      try (var statement = connection.prepareStatement(statementText)) {
        try (var result = statement.executeQuery()) {
          if (!result.next()) {
            throw new SQLException("schema_version table is empty!");
          }
          final var applicationCA =
            result.getString(1);
          final var version =
            result.getLong(2);

          if (!Objects.equals(applicationCA, DATABASE_APPLICATION_ID)) {
            throw new SQLException(
              String.format(
                "Database application ID is %s but should be %s",
                applicationCA,
                DATABASE_APPLICATION_ID
              )
            );
          }

          return Optional.of(valueOf(version));
        }
      }
    } catch (final SQLException e) {
      if (e.getErrorCode() == SQLiteErrorCode.SQLITE_ERROR.code) {
        connection.rollback();
        return Optional.empty();
      }
      throw e;
    }
  }

  private static USQSQLiteCollection connect(
    final Path file)
  {
    final var url = new StringBuilder(128);
    url.append("jdbc:sqlite:");
    url.append(file);

    final var config = new SQLiteConfig();
    config.setApplicationId(APPLICATION_ID);
    config.enforceForeignKeys(true);
    config.setJournalMode(SQLiteConfig.JournalMode.WAL);
    config.setLockingMode(SQLiteConfig.LockingMode.NORMAL);
    config.setSynchronous(SQLiteConfig.SynchronousMode.FULL);

    final var dataSource = new SQLiteDataSource(config);
    dataSource.setUrl(url.toString());
    return new USQSQLiteCollection(dataSource);
  }

  private static void createOrUpgrade(
    final Path file,
    final Consumer<String> startupMessages)
    throws IOException, SQLException, TrException, ParsingException
  {
    final var arguments =
      new TrArguments(Map.of());

    final var url = new StringBuilder(128);
    url.append("jdbc:sqlite:");
    url.append(file);

    final var config = new SQLiteConfig();
    config.setApplicationId(APPLICATION_ID);
    config.enforceForeignKeys(true);
    config.setJournalMode(SQLiteConfig.JournalMode.WAL);
    config.setLockingMode(SQLiteConfig.LockingMode.NORMAL);
    config.setSynchronous(SQLiteConfig.SynchronousMode.FULL);

    final var dataSource = new SQLiteDataSource(config);
    dataSource.setUrl(url.toString());

    final var parsers = new TrSchemaRevisionSetParsers();
    final TrSchemaRevisionSet revisions;
    try (var stream = USQSQLiteCollection.class.getResourceAsStream(
      "/com/io7m/usq/sqlite/internal/Database.xml")) {
      revisions = parsers.parse(URI.create("urn:source"), stream);
    }

    try (var connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);

      new TrExecutors().create(
        new TrExecutorConfiguration(
          USQSQLiteCollection::schemaVersionGet,
          USQSQLiteCollection::schemaVersionSet,
          event -> publishTrEvent(startupMessages, event),
          revisions,
          PERFORM_UPGRADES,
          arguments,
          connection
        )
      ).execute();
      connection.commit();
    }
  }

  private static void publishTrEvent(
    final Consumer<String> startupMessages,
    final TrEventType event)
  {
    switch (event) {
      case final TrEventExecutingSQL sql -> {
        publishEvent(
          startupMessages,
          String.format("Executing SQL: %s", sql.statement())
        );
        return;
      }
      case final TrEventUpgrading upgrading -> {
        publishEvent(
          startupMessages,
          String.format(
            "Upgrading database from version %s -> %s",
            upgrading.fromVersion(),
            upgrading.toVersion())
        );
        return;
      }
    }
  }

  private static void publishEvent(
    final Consumer<String> startupMessages,
    final String message)
  {
    try {
      startupMessages.accept(message);
    } catch (final Exception e) {
      // Don't care.
    }
  }

  /**
   * Open a queue collection.
   *
   * @param configuration The configuration
   *
   * @return A queue collection
   *
   * @throws USQException On errors
   */

  public static USQueueCollectionType open(
    final USQueueCollectionConfigurationType configuration)
    throws USQException
  {
    Objects.requireNonNull(configuration, "configuration");

    try {
      createOrUpgrade(
        configuration.file(),
        configuration.startupEventReceiver()
      );

      final var collection =
        connect(configuration.file());

      final var maintenance =
        collection.resources.add(
          Executors.newSingleThreadScheduledExecutor(r -> {
            return Thread.ofVirtual()
              .name("USQSQLiteCollection-Maintenance-", 0L)
              .unstarted(r);
          })
        );

      maintenance.scheduleAtFixedRate(
        collection::expireOldMessages,
        configuration.messageExpirationInitialDelay().toMillis(),
        configuration.messageExpirationFrequency().toMillis(),
        TimeUnit.MILLISECONDS
      );

      return collection;
    } catch (final IOException e) {
      throw errorIO(e);
    } catch (final SQLException e) {
      throw errorDatabase(e);
    } catch (final TrException e) {
      throw errorDatabaseUpgrade(e);
    } catch (final ParsingException e) {
      throw errorDatabaseUpgradeParse(e);
    }
  }

  private void expireOldMessages()
  {
    if (this.isClosed()) {
      return;
    }

    try {
      final var expired = new HashSet<UUID>();
      this.withConnection(connection -> {
        try (var st =
               connection.prepareStatement(MESSAGE_DELETE_EXPIRED)) {
          st.setString(1, OffsetDateTime.now().toString());
          try (var rs = st.executeQuery()) {
            while (rs.next()) {
              expired.add(UUID.fromString(rs.getString(1)));
            }
          }
        }
        connection.commit();
        return null;
      });

      for (final var id : expired) {
        this.events.submit(new USQEventMessageExpired(id));
      }
    } catch (final Throwable e) {
      this.events.submit(new USQEventMessageExpirationFailed(e));
    }
  }

  static USQException errorDatabase(
    final SQLException e)
  {
    return new USQException(
      "Database error.",
      e,
      "error-database",
      Map.of(),
      Optional.empty()
    );
  }

  static USQException errorIO(
    final IOException e)
  {
    return new USQException(
      "I/O error.",
      e,
      "error-io",
      Map.of(),
      Optional.empty()
    );
  }

  static USQException errorDatabaseUpgradeParse(
    final ParsingException e)
  {
    return new USQException(
      "Parse error.",
      e,
      "error-parse",
      Map.of(),
      Optional.empty()
    );
  }

  static USQException errorDatabaseUpgrade(
    final TrException e)
  {
    return new USQException(
      "Database upgrade error.",
      e,
      "error-database-upgrade",
      Map.of(),
      Optional.empty()
    );
  }

  SubmissionPublisher<USQEventType> eventSource()
  {
    return this.events;
  }

  @Override
  public Flow.Publisher<USQEventType> events()
  {
    return this.events;
  }

  @Override
  public USQueueType openQueue(
    final String name)
    throws USQException, InterruptedException
  {
    Objects.requireNonNull(name, "name");

    this.checkNotClosed();

    return this.queueCreateOrReturnExisting(name);
  }

  @Override
  public boolean isClosed()
  {
    return this.closed.get();
  }

  @Override
  public Set<String> queues()
    throws USQException, InterruptedException
  {
    this.checkNotClosed();

    return this.withConnection(connection -> {
      try (var st =
             connection.prepareStatement(QUEUE_LIST)) {

        final var results = new HashSet<String>();
        try (var rs = st.executeQuery()) {
          while (rs.next()) {
            results.add(rs.getString(1));
          }
        }
        return Set.copyOf(results);
      }
    });
  }

  private void checkNotClosed()
    throws USQException
  {
    if (this.isClosed()) {
      throw new USQException(
        "Queue collection is closed.",
        "error-closed",
        Map.ofEntries(),
        Optional.empty()
      );
    }
  }

  @Override
  public void close()
    throws USQException
  {
    if (this.closed.compareAndSet(false, true)) {
      this.resources.close();
    }
  }

  /**
   * Create a queue with the given name, or return an existing queue instance
   * with the given name.
   *
   * @param name The name
   *
   * @return The queue
   *
   * @throws USQException         On errors
   * @throws InterruptedException On interruption
   */

  USQueueType queueCreateOrReturnExisting(
    final String name)
    throws USQException, InterruptedException
  {
    /*
     * Actually creating a queue is a relatively expensive operation as it
     * may involve reading messages from a database. We optimistically check
     * to see if there's an existing reusable instance to avoid this expensive
     * instance creation.
     */

    this.queuesOpenLock.lockInterruptibly();
    try {
      final var existing = this.queuesOpen.get(name);
      if (existing != null) {
        return existing;
      }
    } finally {
      this.queuesOpenLock.unlock();
    }

    /*
     * There wasn't an existing instance, so create one...
     */

    final var newQueue =
      USQSQLiteQueue.open(this, name);

    /*
     * We now need to check _again_ to see if someone else has created a
     * queue instance in the time since we checked last. If someone has created
     * one, return that instead of the one we created.
     */

    this.queuesOpenLock.lockInterruptibly();
    try {
      final var existing = this.queuesOpen.get(name);
      if (existing != null) {
        return existing;
      }
      this.queuesOpen.put(name, newQueue);
      return newQueue;
    } finally {
      this.queuesOpenLock.unlock();
    }
  }

  void queueDelete(
    final USQSQLiteQueue queue)
    throws InterruptedException
  {
    this.queuesOpenLock.lockInterruptibly();
    try {
      this.queuesOpen.remove(queue.name());
    } finally {
      this.queuesOpenLock.unlock();
    }
  }

  interface WithConnectionType<T>
  {
    T execute(Connection connection)
      throws SQLException, USQException;
  }

  <T> T withConnection(
    final WithConnectionType<T> f)
    throws USQException, InterruptedException
  {
    this.connectionSemaphore.acquire();

    try {
      try (var connection = this.dataSource.getConnection()) {
        connection.setAutoCommit(false);
        return f.execute(connection);
      } catch (final SQLException e) {
        throw errorDatabase(e);
      }
    } finally {
      this.connectionSemaphore.release();
    }
  }
}
