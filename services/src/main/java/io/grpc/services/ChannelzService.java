/*
 * Copyright 2018 The gRPC Authors
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

package io.grpc.services;

import io.grpc.ExperimentalApi;
import io.grpc.InternalChannelz;

/**
 * The channelz service provides stats about a running gRPC process.
 *
 * @deprecated Use {@link io.grpc.protobuf.services.ChannelzService} instead.
 */
@Deprecated
@ExperimentalApi("https://github.com/grpc/grpc-java/issues/4206")
public final class ChannelzService extends io.grpc.protobuf.services.ChannelzService {

  /**
   * Creates an instance.
   */
  public static ChannelzService newInstance(int maxPageSize) {
    return new ChannelzService(InternalChannelz.instance(), maxPageSize);
  }

  private ChannelzService(InternalChannelz channelz, int maxPageSize) {
    super(channelz, maxPageSize);
  }
}
