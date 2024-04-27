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

import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Functions over collection configurations.
 */

public final class USQueueCollectionConfiguration
  implements USQueueCollectionConfigurationType
{
  private final Path file;
  private final Duration messageExpirationFrequency;
  private final Duration messageExpirationInitialDelay;
  private final Consumer<String> startupEventReceiver;

  private USQueueCollectionConfiguration(
    final Path inFile,
    final Duration inMessageExpirationFrequency,
    final Duration inMessageExpirationInitialDelay,
    final Consumer<String> inStartupEventReceiver)
  {
    this.file =
      Objects.requireNonNull(
        inFile, "file");
    this.messageExpirationFrequency =
      Objects.requireNonNull(
        inMessageExpirationFrequency, "messageExpirationFrequency");
    this.messageExpirationInitialDelay =
      Objects.requireNonNull(
        inMessageExpirationInitialDelay, "messageExpirationInitialDelay");
    this.startupEventReceiver =
      Objects.requireNonNull(
        inStartupEventReceiver, "startupEventReceiver");
  }

  /**
   * @return A mutable configuration builder
   */

  public static Builder builder()
  {
    return new Builder();
  }

  @Override
  public Path file()
  {
    return this.file;
  }

  @Override
  public Duration messageExpirationFrequency()
  {
    return this.messageExpirationFrequency;
  }

  @Override
  public Duration messageExpirationInitialDelay()
  {
    return this.messageExpirationInitialDelay;
  }

  @Override
  public Consumer<String> startupEventReceiver()
  {
    return this.startupEventReceiver;
  }

  /**
   * A mutable configuration builder.
   */

  public static final class Builder
  {
    private Path file;
    private Duration messageExpirationFrequency = Duration.ofSeconds(60L);
    private Duration messageExpirationInitialDelay = Duration.ZERO;
    private Consumer<String> startupEventReceiver = ignored -> {
    };

    private Builder()
    {

    }

    /**
     * Set the file.
     *
     * @param inFile The file
     *
     * @return this
     */

    public Builder setFile(
      final Path inFile)
    {
      this.file = Objects.requireNonNull(inFile, "file");
      return this;
    }

    /**
     * Set the message expiration frequency.
     *
     * @param frequency The frequency
     *
     * @return this
     */

    public Builder setMessageExpirationFrequency(
      final Duration frequency)
    {
      this.messageExpirationFrequency =
        Objects.requireNonNull(frequency, "messageExpirationFrequency");
      return this;
    }

    /**
     * Set the message expiration initial delay.
     *
     * @param delay The delay
     *
     * @return this
     */

    public Builder setMessageExpirationInitialDelay(
      final Duration delay)
    {
      this.messageExpirationInitialDelay =
        Objects.requireNonNull(delay, "messageExpirationInitialDelay");
      return this;
    }

    /**
     * Set the startup event receiver.
     *
     * @param receiver The receiver
     *
     * @return this
     */

    public Builder setStartupEventReceiver(
      final Consumer<String> receiver)
    {
      this.startupEventReceiver =
        Objects.requireNonNull(receiver, "startupEventReceiver");
      return this;
    }

    /**
     * Build an immutable configuration.
     *
     * @return The configuration
     */

    public USQueueCollectionConfigurationType build()
    {
      return new USQueueCollectionConfiguration(
        this.file,
        this.messageExpirationFrequency,
        this.messageExpirationInitialDelay,
        this.startupEventReceiver
      );
    }
  }
}
