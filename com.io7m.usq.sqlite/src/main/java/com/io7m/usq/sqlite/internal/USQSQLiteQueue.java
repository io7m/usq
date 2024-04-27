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

import com.io7m.usq.api.USQEventMessageEnqueued;
import com.io7m.usq.api.USQEventQueueCreated;
import com.io7m.usq.api.USQEventQueueDeleted;
import com.io7m.usq.api.USQException;
import com.io7m.usq.api.USQMessage;
import com.io7m.usq.api.USQueueType;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An SQLite-based queue.
 */

public final class USQSQLiteQueue implements USQueueType
{
  private static final String JAVA_PROPERTIES_FORMAT =
    "text/x-java-properties";

  private static final String QUEUE_CREATE = """
    INSERT INTO queues (q_name) VALUES (?) RETURNING q_dbid
    """.trim();

  private static final String QUEUE_GET = """
    SELECT queues.q_dbid FROM queues WHERE queues.q_name = ?
    """.trim();

  private static final String QUEUE_ALL_IDS = """
    SELECT
      messages.msg_id
    FROM messages
    JOIN     queue_messages ON queue_messages.qm_message = messages.msg_dbid
    WHERE    queue_messages.qm_queue = ?
    ORDER BY queue_messages.qm_order ASC
    """.trim();

  private static final String QUEUE_DELETE = """
    DELETE FROM queues WHERE queues.q_dbid = ?
    """.trim();

  private static final String MESSAGE_CREATE = """
    INSERT INTO messages
      (msg_id,
       msg_created_on,
       msg_expires_on,
       msg_metadata_format,
       msg_metadata,
       msg_data)
    VALUES
      (?,
       ?,
       ?,
       ?,
       ?,
       ?)
    RETURNING
      msg_dbid
    """.trim();

  private static final String MESSAGE_GET_ID = """
    SELECT messages.msg_dbid FROM messages WHERE messages.msg_id = ?
    """.trim();

  private static final String MESSAGE_ENQUEUE = """
    INSERT INTO queue_messages
      (qm_queue,
       qm_message)
    VALUES
      (?, ?)
    """.trim();

  private static final String MESSAGE_OLDEST = """
    SELECT
      messages.msg_id,
      messages.msg_created_on,
      messages.msg_expires_on,
      messages.msg_metadata_format,
      messages.msg_metadata,
      messages.msg_data
    FROM messages
    JOIN     queue_messages ON queue_messages.qm_message = messages.msg_dbid
    WHERE    queue_messages.qm_queue = ?
    ORDER BY queue_messages.qm_order ASC
    LIMIT 1
    """.trim();

  private static final String MESSAGE_DEQUEUE = """
    DELETE FROM queue_messages
      WHERE queue_messages.qm_queue = ?
        AND queue_messages.qm_message =
          (SELECT messages.msg_dbid FROM messages WHERE messages.msg_id = ?)
    """.trim();

  private final USQSQLiteCollection queueCollection;
  private final String name;
  private final long queueId;
  private final AtomicBoolean deleted;
  private final LinkedBlockingQueue<UUID> writes;

  private USQSQLiteQueue(
    final USQSQLiteCollection inQueueCollection,
    final String inName,
    final long inQueueId)
  {
    this.queueCollection =
      Objects.requireNonNull(inQueueCollection, "queueCollection");
    this.name =
      Objects.requireNonNull(inName, "name");
    this.queueId =
      inQueueId;
    this.deleted =
      new AtomicBoolean(false);
    this.writes =
      new LinkedBlockingQueue<>();
  }

  /**
   * Create a queue.
   *
   * @param queueCollection The queue collection
   * @param name            The queue name
   *
   * @return The queue
   *
   * @throws USQException         On errors
   * @throws InterruptedException On interruption
   */

  public static USQSQLiteQueue open(
    final USQSQLiteCollection queueCollection,
    final String name)
    throws USQException, InterruptedException
  {
    Objects.requireNonNull(queueCollection, "queueCollection");
    Objects.requireNonNull(name, "name");

    final var created =
      new AtomicBoolean(false);

    final var queue = queueCollection.withConnection(conn -> {
      OptionalLong idOpt = OptionalLong.empty();

      try (var st = conn.prepareStatement(QUEUE_GET)) {
        st.setString(1, name);
        try (var rs = st.executeQuery()) {
          if (rs.next()) {
            idOpt = OptionalLong.of(rs.getLong(1));
          }
        }
      }

      if (idOpt.isEmpty()) {
        try (var st = conn.prepareStatement(QUEUE_CREATE)) {
          st.setString(1, name);
          try (var rs = st.executeQuery()) {
            idOpt = OptionalLong.of(rs.getLong(1));
            created.set(true);
          }
        }
      }

      conn.commit();

      final var id = idOpt.getAsLong();
      final var resultQueue = new USQSQLiteQueue(queueCollection, name, id);
      try (var st = conn.prepareStatement(QUEUE_ALL_IDS)) {
        st.setLong(1, id);
        try (var rs = st.executeQuery()) {
          while (rs.next()) {
            resultQueue.writes.add(UUID.fromString(rs.getString(1)));
          }
        }
      }

      return resultQueue;
    });

    if (created.get()) {
      queueCollection.eventSource().submit(new USQEventQueueCreated(name));
    }

    return queue;
  }

  private static Map<String, String> fromProperties(
    final byte[] bytes)
    throws IOException
  {
    final var results = new HashMap<String, String>();
    final var props = new Properties();
    try (var stream = new ByteArrayInputStream(bytes)) {
      props.load(stream);
      for (final var name : props.stringPropertyNames()) {
        results.put(name, props.getProperty(name));
      }
    }
    return Map.copyOf(results);
  }

  private static byte[] toProperties(
    final Map<String, String> metadata)
    throws IOException
  {
    final var props = new Properties();
    props.putAll(metadata);
    try (var stream = new ByteArrayOutputStream()) {
      props.store(stream, "");
      stream.flush();
      return stream.toByteArray();
    }
  }

  @Override
  public String name()
  {
    return this.name;
  }

  @Override
  public void put(
    final USQMessage message)
    throws USQException, InterruptedException
  {
    Objects.requireNonNull(message, "message");

    this.checkNotDeleted();

    this.queueCollection.withConnection(conn -> {
      try {
        OptionalLong messageDBID = OptionalLong.empty();

        try (var st = conn.prepareStatement(MESSAGE_GET_ID)) {
          st.setString(1, message.id().toString());

          try (var rs = st.executeQuery()) {
            if (rs.next()) {
              messageDBID = OptionalLong.of(rs.getLong(1));
            }
          }
        }

        if (messageDBID.isEmpty()) {
          try (var st = conn.prepareStatement(MESSAGE_CREATE)) {
            st.setString(1, message.id().toString());
            st.setString(2, message.createdOn().toString());
            st.setString(3, message.expiresOn().toString());
            st.setString(4, JAVA_PROPERTIES_FORMAT);
            st.setBytes(5, toProperties(message.metadata()));
            st.setBytes(6, message.data());

            try (var rs = st.executeQuery()) {
              messageDBID = OptionalLong.of(rs.getLong(1));
            }
          } catch (final IOException e) {
            throw USQSQLiteCollection.errorIO(e);
          }
        }

        try (var st = conn.prepareStatement(MESSAGE_ENQUEUE)) {
          st.setLong(1, this.queueId);
          st.setLong(2, messageDBID.getAsLong());
          st.executeUpdate();
        }

        conn.commit();
        return null;
      } catch (final SQLException e) {
        if (e instanceof final SQLiteException se) {
          if (se.getResultCode() == SQLiteErrorCode.SQLITE_CONSTRAINT_UNIQUE) {
            throw this.errorDuplicateMessage(message);
          }
        }
        throw USQSQLiteCollection.errorDatabase(e);
      }
    });

    this.queueCollection.eventSource()
      .submit(new USQEventMessageEnqueued(this.name, message));

    this.notifyWrite(message.id());
  }

  private void notifyWrite(
    final UUID id)
  {
    try {
      this.writes.add(id);
    } catch (final IllegalStateException e) {
      // Not a problem.
    }
  }

  private USQException errorDuplicateMessage(
    final USQMessage message)
  {
    return new USQException(
      "A message with the given ID already exists in the queue.",
      "error-message-duplicate",
      Map.ofEntries(
        Map.entry("Queue", this.name),
        Map.entry("MessageID", message.id().toString())
      ),
      Optional.empty()
    );
  }

  @Override
  public Optional<USQMessage> peek(
    final Duration timeout)
    throws USQException, InterruptedException
  {
    this.checkNotDeleted();
    this.writes.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);

    return this.queueCollection.withConnection(conn -> {
      try (var st = conn.prepareStatement(MESSAGE_OLDEST)) {
        st.setLong(1, this.queueId);

        try (var rs = st.executeQuery()) {
          if (rs.next()) {
            final var format = rs.getString(4);
            return switch (format) {
              case JAVA_PROPERTIES_FORMAT -> {
                yield Optional.of(
                  new USQMessage(
                    UUID.fromString(rs.getString(1)),
                    OffsetDateTime.parse(rs.getString(2)),
                    OffsetDateTime.parse(rs.getString(3)),
                    fromProperties(rs.getBytes(5)),
                    rs.getBytes(6)
                  )
                );
              }
              default -> {
                throw new IOException(
                  "Unrecognized message metadata format: %s".formatted(format)
                );
              }
            };
          }
          return Optional.empty();
        } catch (final IOException e) {
          throw USQSQLiteCollection.errorIO(e);
        }
      }
    });
  }

  @Override
  public boolean remove(
    final UUID id)
    throws USQException, InterruptedException
  {
    Objects.requireNonNull(id, "id");

    this.checkNotDeleted();

    return this.queueCollection.<Boolean>withConnection(conn -> {
      final int dequeued;
      try (var st = conn.prepareStatement(MESSAGE_DEQUEUE)) {
        st.setLong(1, this.queueId);
        st.setString(2, id.toString());
        dequeued = st.executeUpdate();
      }

      conn.commit();
      return Boolean.valueOf(dequeued > 0);
    }).booleanValue();
  }

  private void checkNotDeleted()
    throws USQException
  {
    if (this.deleted.get()) {
      throw new USQException(
        "Queue has been deleted.",
        "error-deleted",
        Map.ofEntries(),
        Optional.empty()
      );
    }
  }

  @Override
  public void delete()
    throws USQException, InterruptedException
  {
    if (this.deleted.compareAndSet(false, true)) {
      this.queueCollection.withConnection(conn -> {
        try (var st = conn.prepareStatement(QUEUE_DELETE)) {
          st.setLong(1, this.queueId);
          st.executeUpdate();
        }
        conn.commit();
        return null;
      });

      this.queueCollection.queueDelete(this);
      this.queueCollection.eventSource()
        .submit(new USQEventQueueDeleted(this.name));
    }
  }

  @Override
  public String toString()
  {
    return "[USQSQLiteQueue 0x%s %s]"
      .formatted(
        Long.toUnsignedString(this.queueId, 16),
        this.name
      );
  }
}
