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

import com.io7m.usq.api.USQueueCollectionConfiguration;
import com.io7m.usq.api.USQueueCollectionType;
import com.io7m.usq.sqlite.USQSQLiteProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;

public final class USQueueCollectionSmokeMessagesSQLiteIT
  extends USQueueCollectionSmokeContract
{
  private static final Logger LOG =
    LoggerFactory.getLogger(USQueueCollectionSmokeMessagesSQLiteIT.class);

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
        .setMessageExpirationFrequency(Duration.ofMillis(100L))
        .setMessageExpirationInitialDelay(Duration.ofMillis(1L))
        .setStartupEventReceiver(s -> LOG.debug("{}", s))
        .build();

    return provider.open(configuration);
  }
}
