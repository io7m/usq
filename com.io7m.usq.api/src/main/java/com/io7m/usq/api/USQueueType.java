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

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * A queue.
 */

public interface USQueueType
{
  /**
   * @return The name of this queue
   */

  String name();

  /**
   * Place a message in the queue.
   *
   * @param message The message
   *
   * @throws USQException         On errors
   * @throws InterruptedException On interruption
   */

  void put(
    USQMessage message)
    throws USQException, InterruptedException;

  /**
   * Peek at the head of the queue. If the queue has no items in it,
   * wait up to the specified duration for the queue to become non-empty.
   *
   * @param timeout The maximum duration to wait for an item to appear in the queue
   *
   * @return The message at the head of the queue, if any
   *
   * @throws USQException         On errors
   * @throws InterruptedException On interruption
   */

  Optional<USQMessage> peek(Duration timeout)
    throws USQException, InterruptedException;

  /**
   * Remove the message with the given ID from the queue.
   *
   * @param id The message ID
   *
   * @return {@code true} if a message was removed
   *
   * @throws USQException         On errors
   * @throws InterruptedException On interruption
   */

  boolean remove(UUID id)
    throws USQException, InterruptedException;

  /**
   * Delete this queue.
   *
   * @throws USQException         On errors
   * @throws InterruptedException On interruption
   */

  void delete()
    throws USQException, InterruptedException;
}
