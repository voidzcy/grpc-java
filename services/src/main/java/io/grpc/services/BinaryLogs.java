/*
 * Copyright 2018, gRPC Authors All rights reserved.
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

/**
 * @deprecated Use {@link io.grpc.protobuf.services.BinaryLogs} instead.
 */
@Deprecated
@ExperimentalApi("https://github.com/grpc/grpc-java/issues/4017")
public final class BinaryLogs extends io.grpc.protobuf.services.BinaryLogs {
  private BinaryLogs() {}
}
