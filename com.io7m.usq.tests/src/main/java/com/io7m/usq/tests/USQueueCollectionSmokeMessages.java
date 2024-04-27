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

import com.io7m.usq.api.USQMessage;
import com.io7m.usq.api.USQueueCollectionType;
import com.io7m.usq.api.USQueueType;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class USQueueCollectionSmokeMessages implements Callable<Void>
{
  private final Logger logger;
  private final USQueueCollectionType collection;
  private final int messageCount;
  private final int producerCount;
  private final int consumerCount;
  private final ConcurrentHashMap<UUID, USQMessage> messagesReceived;
  private final Map<UUID, USQMessage> messagesToSend;
  private final int queueCount;

  public USQueueCollectionSmokeMessages(
    final Logger inLogger,
    final USQueueCollectionType inCollection,
    final int messageCount,
    final int producerCount,
    final int consumerCount,
    final int queueCount)
  {
    this.logger =
      Objects.requireNonNull(inLogger, "inLogger");
    this.collection =
      Objects.requireNonNull(inCollection, "collection");

    this.messageCount = messageCount;
    this.producerCount = producerCount;
    this.consumerCount = consumerCount;
    this.queueCount = queueCount;

    this.messagesReceived =
      new ConcurrentHashMap<>();
    this.messagesToSend =
      IntStream.rangeClosed(0, this.messageCount)
        .mapToObj(i -> {
          return new USQMessage(
            UUID.randomUUID(),
            OffsetDateTime.now(),
            OffsetDateTime.now().plusYears(1L),
            Map.of(),
            Integer.toUnsignedString(i).getBytes(StandardCharsets.UTF_8)
          );
        })
        .collect(Collectors.toMap(USQMessage::id, x -> x));
  }

  private static USQueueType pickQueue(
    final ConcurrentHashMap<String, USQueueType> queues)
  {
    final var queueList = new ArrayList<>(queues.values());
    Collections.shuffle(queueList);
    return queueList.get(0);
  }

  private static USQMessage pickMessage(
    final ConcurrentHashMap<UUID, USQMessage> messagePool)
  {
    final var key =
      messagePool.keys()
        .nextElement();

    if (key != null) {
      return messagePool.remove(key);
    }
    return null;
  }

  public Map<UUID, USQMessage> messagesReceived()
  {
    return Map.copyOf(this.messagesReceived);
  }

  public Map<UUID, USQMessage> messagesSent()
  {
    return Map.copyOf(this.messagesToSend);
  }

  private void producerTask(
    final CompletableFuture<Void> future,
    final ConcurrentHashMap<String, USQueueType> queues,
    final ConcurrentHashMap<UUID, USQMessage> messagePool)
  {
    Thread.startVirtualThread(() -> {
      try {
        while (!this.collection.isClosed()) {
          try {
            if (messagePool.isEmpty()) {
              return;
            }

            final var message =
              pickMessage(messagePool);

            if (message == null) {
              continue;
            }

            final var queue =
              pickQueue(queues);

            queue.put(message);
          } catch (final Exception e) {
            this.logger.debug("Error: ", e);
          }
        }
      } finally {
        this.logger.debug("Producer finished.");
        future.complete(null);
      }
    });
  }

  private void consumerTask(
    final CompletableFuture<Void> future,
    final ConcurrentHashMap<String, USQueueType> queues,
    final int expectedSize)
  {
    Thread.startVirtualThread(() -> {
      try {
        while (this.messagesReceived.size() < expectedSize) {
          try {
            for (final var q : queues.values()) {
              final var messageOpt =
                q.peek(Duration.ofMillis(100L));

              if (messageOpt.isPresent()) {
                final var message = messageOpt.get();
                this.messagesReceived.put(message.id(), message);
                q.remove(message.id());
              }
            }
          } catch (final Exception e) {
            this.logger.debug("Error: ", e);
          }
        }
      } finally {
        this.logger.debug("Consumer finished.");
        future.complete(null);
      }
    });
  }

  @Override
  public Void call()
    throws Exception
  {
    final var messagePool =
      new ConcurrentHashMap<>(this.messagesToSend);
    final var queues =
      new ConcurrentHashMap<String, USQueueType>();

    Thread.startVirtualThread(() -> {
      while (!this.collection.isClosed()) {
        this.logger
          .debug("Received size: {}", Integer.valueOf(this.messagesReceived.size()));
        try {
          Thread.sleep(1_000L);
        } catch (final InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    });

    for (int index = 0; index < this.queueCount; ++index) {
      final var name = "Q" + index;
      queues.put(name, this.collection.openQueue(name));
    }

    final var consumerFutures =
      new ArrayList<CompletableFuture<Void>>();

    for (int index = 0; index < this.consumerCount; ++index) {
      final var future = new CompletableFuture<Void>();
      consumerFutures.add(future);
      this.consumerTask(future, queues, messagePool.size());
    }

    final var producerFutures =
      new ArrayList<CompletableFuture<Void>>();

    for (int index = 0; index < this.producerCount; ++index) {
      final var future = new CompletableFuture<Void>();
      producerFutures.add(future);
      this.producerTask(future, queues, messagePool);
    }

    final var allProducerFutures =
      CompletableFuture.allOf(
        producerFutures.toArray(new CompletableFuture[0])
      );

    allProducerFutures.get();

    final var allConsumerFutures =
      CompletableFuture.allOf(
        consumerFutures.toArray(new CompletableFuture[0])
      );

    allConsumerFutures.get();
    return null;
  }

  public void validate()
    throws Exception
  {
    final var sent =
      this.messagesSent();
    final var received =
      this.messagesReceived();

    Exception failure = null;
    if (received.size() < sent.size()) {
      failure = new IllegalStateException("One or more issues occurred.");

      this.logger.error(
        "Received count {} is less than sent count {}",
        Integer.valueOf(received.size()),
        Integer.valueOf(sent.size())
      );

      for (final var entry : sent.entrySet()) {
        final var id = entry.getKey();
        if (!received.containsKey(id)) {
          this.logger.error("Missing message: {}", id);
        }
      }
    }

    if (received.size() > sent.size()) {
      failure = new IllegalStateException("One or more issues occurred.");

      this.logger.error(
        "Received count {} is greater than sent count {}",
        Integer.valueOf(received.size()),
        Integer.valueOf(sent.size())
      );

      for (final var entry : received.entrySet()) {
        final var id = entry.getKey();
        if (!sent.containsKey(id)) {
          this.logger.error("Phantom message: {}", id);
        }
      }
    }

    if (received.size() == sent.size()) {
      this.logger.info(
        "Received count {} equals sent count {}",
        Integer.valueOf(received.size()),
        Integer.valueOf(sent.size())
      );
    }

    if (failure != null) {
      throw failure;
    }
  }
}
