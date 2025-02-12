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


package com.io7m.usq.api;

import java.util.Set;
import java.util.concurrent.Flow;

/**
 * The type of queue collections.
 */

public interface USQueueCollectionType
  extends AutoCloseable
{
  /**
   * @return An observable stream of events
   */

  Flow.Publisher<USQEventType> events();

  /**
   * Create a new queue, or get an existing queue with the given name.
   *
   * @param name The name
   *
   * @return The queue
   *
   * @throws USQException On errors
   */

  USQueueType openQueue(String name)
    throws USQException, InterruptedException;

  /**
   * @return {@code true} if this queue collection has been closed
   */

  boolean isClosed();

  /**
   * @return The names of all queues
   *
   * @throws USQException On errors
   */

  Set<String> queues()
    throws USQException, InterruptedException;

  @Override
  void close()
    throws USQException;
}
