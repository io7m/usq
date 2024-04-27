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

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * The type of messages that can be placed in queues.
 *
 * @param id        The message ID
 * @param createdOn The time/date the message was created
 * @param expiresOn The time/date the message expires
 * @param metadata  The message metadata
 * @param data      The serialized message bytes
 */

public record USQMessage(
  UUID id,
  OffsetDateTime createdOn,
  OffsetDateTime expiresOn,
  Map<String, String> metadata,
  byte[] data)
{
  /**
   * The type of messages that can be placed in queues.
   *
   * @param id        The message ID
   * @param createdOn The time/date the message was created
   * @param expiresOn The time/date the message expires
   * @param metadata  The message metadata
   * @param data      The serialized message bytes
   */

  public USQMessage
  {
    Objects.requireNonNull(id, "id");
    Objects.requireNonNull(createdOn, "createdOn");
    Objects.requireNonNull(expiresOn, "expiresOn");
    Objects.requireNonNull(metadata, "metadata");
    Objects.requireNonNull(data, "data");
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || !this.getClass().equals(o.getClass())) {
      return false;
    }
    final USQMessage that = (USQMessage) o;
    return Objects.equals(this.id, that.id)
           && Objects.equals(this.createdOn, that.createdOn)
           && Objects.equals(this.expiresOn, that.expiresOn)
           && Objects.equals(this.metadata, that.metadata)
           && Arrays.equals(this.data, that.data);
  }

  @Override
  public int hashCode()
  {
    int result = Objects.hash(
      this.id,
      this.createdOn,
      this.expiresOn,
      this.metadata);
    result = 31 * result + Arrays.hashCode(this.data);
    return result;
  }
}
