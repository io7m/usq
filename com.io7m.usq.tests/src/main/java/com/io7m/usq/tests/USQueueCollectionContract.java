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

import com.io7m.usq.api.USQEventMessageEnqueued;
import com.io7m.usq.api.USQEventMessageExpired;
import com.io7m.usq.api.USQEventQueueCreated;
import com.io7m.usq.api.USQEventQueueDeleted;
import com.io7m.usq.api.USQEventType;
import com.io7m.usq.api.USQException;
import com.io7m.usq.api.USQMessage;
import com.io7m.usq.api.USQueueCollectionType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;

import java.nio.file.Path;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class USQueueCollectionContract
  implements Flow.Subscriber<USQEventType>
{
  private static final Duration PEEK_TIME =
    Duration.ofSeconds(1L);

  private Path file;
  private LinkedBlockingQueue<USQEventType> events;

  protected abstract Logger logger();

  protected abstract USQueueCollectionType
  createCollection(
    Path file)
    throws Exception;

  protected abstract USQueueCollectionType
  createCollectionWithImmediateExpiration(
    Path file)
    throws Exception;

  @BeforeEach
  public final void setup(
    final @TempDir Path directory)
  {
    this.file =
      directory.resolve("database.db");
    this.events =
      new LinkedBlockingQueue<>();
  }

  @Test
  public final void testOpenClose()
    throws Exception
  {
    try (var c = this.createCollection(this.file)) {
      c.events().subscribe(this);

      assertEquals(Set.of(), c.queues());
    }

    assertEquals(0, this.events.size());
  }

  @Test
  public final void testOpenCreateQueue()
    throws Exception
  {
    try (var c = this.createCollection(this.file)) {
      c.events().subscribe(this);

      final var q1 = c.openQueue("Q1");
      this.logger().debug("Q1: {}", q1);
      final var q2 = c.openQueue("Q2");
      this.logger().debug("Q2: {}", q2);
      final var q3 = c.openQueue("Q1");
      this.logger().debug("Q3: {}", q3);
      assertEquals(Set.of("Q1", "Q2"), c.queues());
    }

    assertEquals(new USQEventQueueCreated("Q1"), this.events.poll());
    assertEquals(new USQEventQueueCreated("Q2"), this.events.poll());
    assertEquals(0, this.events.size());
  }

  @Test
  public final void testOpenCreateQueueLeaked()
    throws Exception
  {
    final USQueueCollectionType leaked;

    try (var c = this.createCollection(this.file)) {
      c.events().subscribe(this);

      leaked = c;
      final var q1 = c.openQueue("Q1");
      this.logger().debug("Q1: {}", q1);
      final var q2 = c.openQueue("Q2");
      this.logger().debug("Q2: {}", q2);
      final var q3 = c.openQueue("Q1");
      this.logger().debug("Q3: {}", q3);
      assertEquals(Set.of("Q1", "Q2"), c.queues());
    }

    final var ex = assertThrows(USQException.class, () -> {
      leaked.openQueue("x");
    });
    assertEquals("error-closed", ex.errorCode());

    assertEquals(new USQEventQueueCreated("Q1"), this.events.poll());
    assertEquals(new USQEventQueueCreated("Q2"), this.events.poll());
    assertEquals(0, this.events.size());
  }

  @Test
  public final void testOpenCreateQueuePostMessage()
    throws Exception
  {
    final var m =
      new USQMessage(
        UUID.randomUUID(),
        OffsetDateTime.parse("2000-01-01T00:00:00+00:00"),
        OffsetDateTime.parse("2000-01-02T00:00:00+00:00"),
        Map.ofEntries(
          Map.entry("CorrelationID", UUID.randomUUID().toString()),
          Map.entry("X", "A"),
          Map.entry("Y", "B"),
          Map.entry("Z", "C")
        ),
        new byte[0]
      );

    try (var c = this.createCollection(this.file)) {
      c.events().subscribe(this);

      final var q = c.openQueue("Q1");
      q.put(m);
      assertEquals(Optional.of(m), q.peek(PEEK_TIME));

      assertTrue(q.remove(m.id()), "Message was taken");
      assertEquals(Optional.empty(), q.peek(PEEK_TIME));

      assertFalse(q.remove(m.id()), "Message was not taken");
      assertEquals(Optional.empty(), q.peek(PEEK_TIME));
    }

    assertEquals(new USQEventQueueCreated("Q1"), this.events.poll());
    assertEquals(new USQEventMessageEnqueued("Q1", m), this.events.poll());
    assertEquals(0, this.events.size());
  }

  @Test
  public final void testOpenCreateQueuePostMessageDuplicate()
    throws Exception
  {
    final var m =
      new USQMessage(
        UUID.randomUUID(),
        OffsetDateTime.parse("2000-01-01T00:00:00+00:00"),
        OffsetDateTime.parse("2000-01-02T00:00:00+00:00"),
        Map.ofEntries(
          Map.entry("CorrelationID", UUID.randomUUID().toString()),
          Map.entry("X", "A"),
          Map.entry("Y", "B"),
          Map.entry("Z", "C")
        ),
        new byte[0]
      );

    try (var c = this.createCollection(this.file)) {
      c.events().subscribe(this);

      final var q = c.openQueue("Q1");
      q.put(m);

      final var ex =
        assertThrows(USQException.class, () -> q.put(m));
      assertEquals("error-message-duplicate", ex.errorCode());
      Thread.sleep(100L);
    }

    assertEquals(new USQEventQueueCreated("Q1"), this.events.poll());
    assertEquals(new USQEventMessageEnqueued("Q1", m), this.events.poll());
    assertEquals(0, this.events.size());
  }

  @Test
  public final void testOpenCreateQueuesPostMessageRemove()
    throws Exception
  {
    final var m =
      new USQMessage(
        UUID.randomUUID(),
        OffsetDateTime.parse("2000-01-01T00:00:00+00:00"),
        OffsetDateTime.parse("2000-01-02T00:00:00+00:00"),
        Map.ofEntries(
          Map.entry("CorrelationID", UUID.randomUUID().toString()),
          Map.entry("X", "A"),
          Map.entry("Y", "B"),
          Map.entry("Z", "C")
        ),
        new byte[0]
      );

    try (var c = this.createCollection(this.file)) {
      c.events().subscribe(this);

      final var q0 = c.openQueue("Q0");
      final var q1 = c.openQueue("Q1");
      final var q2 = c.openQueue("Q2");

      q0.put(m);
      q1.put(m);
      q2.put(m);

      assertEquals(Optional.of(m), q0.peek(PEEK_TIME));
      assertEquals(Optional.of(m), q1.peek(PEEK_TIME));
      assertEquals(Optional.of(m), q2.peek(PEEK_TIME));

      final var r = q0.remove(m.id());
      assertTrue(r, "Message removed");

      assertEquals(Optional.empty(), q0.peek(PEEK_TIME));
      assertEquals(Optional.of(m), q1.peek(PEEK_TIME));
      assertEquals(Optional.of(m), q2.peek(PEEK_TIME));
    }

    assertEquals(new USQEventQueueCreated("Q0"), this.events.poll());
    assertEquals(new USQEventQueueCreated("Q1"), this.events.poll());
    assertEquals(new USQEventQueueCreated("Q2"), this.events.poll());
    assertEquals(new USQEventMessageEnqueued("Q0", m), this.events.poll());
    assertEquals(new USQEventMessageEnqueued("Q1", m), this.events.poll());
    assertEquals(new USQEventMessageEnqueued("Q2", m), this.events.poll());
    assertEquals(0, this.events.size());
  }

  @Test
  public final void testOpenCreateQueueDelete()
    throws Exception
  {
    final var m =
      new USQMessage(
        UUID.randomUUID(),
        OffsetDateTime.parse("2000-01-01T00:00:00+00:00"),
        OffsetDateTime.parse("2000-01-02T00:00:00+00:00"),
        Map.ofEntries(
          Map.entry("CorrelationID", UUID.randomUUID().toString()),
          Map.entry("X", "A"),
          Map.entry("Y", "B"),
          Map.entry("Z", "C")
        ),
        new byte[0]
      );

    try (var c = this.createCollection(this.file)) {
      c.events().subscribe(this);

      final var q = c.openQueue("Q1");
      q.put(m);
      q.delete();

      final var ex =
        assertThrows(USQException.class, () -> q.put(m));
      assertEquals("error-deleted", ex.errorCode());
      Thread.sleep(100L);
    }

    assertEquals(new USQEventQueueCreated("Q1"), this.events.poll());
    assertEquals(new USQEventMessageEnqueued("Q1", m), this.events.poll());
    assertEquals(new USQEventQueueDeleted("Q1"), this.events.poll());
    assertEquals(0, this.events.size());
  }

  @Test
  public final void testExpiration()
    throws Exception
  {
    final var m =
      new USQMessage(
        UUID.randomUUID(),
        OffsetDateTime.parse("2000-01-01T00:00:00+00:00"),
        OffsetDateTime.parse("2000-01-02T00:00:00+00:00"),
        Map.ofEntries(
          Map.entry("CorrelationID", UUID.randomUUID().toString()),
          Map.entry("X", "A"),
          Map.entry("Y", "B"),
          Map.entry("Z", "C")
        ),
        new byte[0]
      );

    try (var c =
           this.createCollectionWithImmediateExpiration(this.file)) {
      c.events().subscribe(this);

      final var q = c.openQueue("Q0");
      q.put(m);

      Thread.sleep(2_000L);
    }

    assertEquals(new USQEventQueueCreated("Q0"), this.events.poll());
    assertEquals(new USQEventMessageEnqueued("Q0", m), this.events.poll());
    assertEquals(new USQEventMessageExpired(m.id()), this.events.poll());
    assertEquals(0, this.events.size());
  }

  @Test
  public final void testOpenCreateQueueDeleteMulti()
    throws Exception
  {
    try (var c = this.createCollection(this.file)) {
      c.events().subscribe(this);

      final var futures = new CompletableFuture[1000];
      try (var exec = Executors.newVirtualThreadPerTaskExecutor()) {
        for (int index = 0; index < futures.length; ++index) {
          final var future = new CompletableFuture<Void>();
          futures[index] = future;
          exec.execute(() -> {
            try {
              final var q = c.openQueue("Q1");
              Thread.sleep((long) (Math.random() * 100L));
              q.delete();
            } catch (final Throwable e) {
              future.completeExceptionally(e);
            } finally {
              future.complete(null);
            }
          });
        }

        CompletableFuture.allOf(futures)
          .get();
      }
    }

    assertEquals(new USQEventQueueCreated("Q1"), this.events.poll());
    assertEquals(new USQEventQueueDeleted("Q1"), this.events.poll());
    assertEquals(0, this.events.size());
  }

  @Override
  public final void onSubscribe(
    final Flow.Subscription subscription)
  {
    subscription.request(Long.MAX_VALUE);
  }

  @Override
  public final void onNext(
    final USQEventType item)
  {
    this.logger().debug("onNext: {}", item);
    this.events.add(item);
  }

  @Override
  public final void onError(
    final Throwable throwable)
  {

  }

  @Override
  public final void onComplete()
  {

  }
}
