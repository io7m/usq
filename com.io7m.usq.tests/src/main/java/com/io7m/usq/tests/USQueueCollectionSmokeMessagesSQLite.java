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
import com.io7m.usq.api.USQEventMessageExpirationFailed;
import com.io7m.usq.api.USQEventMessageExpired;
import com.io7m.usq.api.USQEventQueueCreated;
import com.io7m.usq.api.USQEventQueueDeleted;
import com.io7m.usq.api.USQEventType;
import com.io7m.usq.api.USQueueCollectionConfiguration;
import com.io7m.usq.sqlite.USQSQLiteProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.Flow;

public final class USQueueCollectionSmokeMessagesSQLite
  implements Flow.Subscriber<USQEventType>
{
  private static final Logger LOG =
    LoggerFactory.getLogger(USQueueCollectionSmokeMessagesSQLite.class);

  private USQueueCollectionSmokeMessagesSQLite()
  {

  }

  public static void main(
    final String[] args)
    throws Exception
  {
    final var file =
      Paths.get(args[0]);
    final var messageCount =
      Integer.parseUnsignedInt(args[1]);
    final var producerCount =
      Integer.parseUnsignedInt(args[2]);
    final var consumerCount =
      Integer.parseUnsignedInt(args[3]);
    final var queueCount =
      Integer.parseUnsignedInt(args[4]);

    Files.deleteIfExists(file);

    LOG.debug("Message count  : {}", Integer.valueOf(messageCount));
    LOG.debug("Producer count : {}", Integer.valueOf(producerCount));
    LOG.debug("Consumer count : {}", Integer.valueOf(consumerCount));
    LOG.debug("Queue count    : {}", Integer.valueOf(queueCount));

    final var provider =
      new USQSQLiteProvider();

    final var configuration =
      USQueueCollectionConfiguration.builder()
        .setFile(file)
        .build();

    LOG.debug("Waiting for keypress...");
    System.in.read();

    try (var collection = provider.open(configuration)) {
      collection.events().subscribe(new USQueueCollectionSmokeMessagesSQLite());

      final var smoke =
        new USQueueCollectionSmokeMessages(
          LOG,
          collection,
          messageCount,
          producerCount,
          consumerCount,
          queueCount
        );

      smoke.call();
      smoke.validate();
    }
  }

  @Override
  public void onSubscribe(
    final Flow.Subscription subscription)
  {
    subscription.request(Long.MAX_VALUE);
  }

  @Override
  public void onNext(
    final USQEventType item)
  {
    switch (item) {
      case final USQEventMessageEnqueued e -> {

      }
      case final USQEventMessageExpirationFailed e -> {

      }
      case final USQEventMessageExpired e -> {
        LOG.info("Expired: {}", e);
      }
      case final USQEventQueueCreated e -> {

      }
      case final USQEventQueueDeleted e -> {

      }
    }
  }

  @Override
  public void onError(
    final Throwable throwable)
  {

  }

  @Override
  public void onComplete()
  {

  }
}
