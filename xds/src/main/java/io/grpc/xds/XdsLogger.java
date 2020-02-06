/*
 * Copyright 2020 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.xds;

import com.google.common.base.Preconditions;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

final class XdsLogger extends Logger {
  private final String prefix;
  private final Logger delegate;

  XdsLogger(String prefix, String delegateName) {
    super("io.grpc.xds.XdsLogger", null);
    this.prefix = Preconditions.checkNotNull(prefix, "prefix");
    delegate = Logger.getLogger(Preconditions.checkNotNull(delegateName, "delegateName"));
    setParent(delegate.getParent());
  }

  @Override
  public void log(LogRecord record) {
    if (delegate.isLoggable(record.getLevel())) {
      record.getSourceMethodName();
      record.setMessage("[" + prefix + "]: " + record.getMessage());
      delegate.log(record);
    }
  }
}
