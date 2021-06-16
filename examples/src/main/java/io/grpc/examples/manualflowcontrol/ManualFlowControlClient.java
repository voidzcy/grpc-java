/*
 * Copyright 2017 The gRPC Authors
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

package io.grpc.examples.manualflowcontrol;

import io.grpc.Codec;
import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ManualFlowControlClient {
    private static final Logger logger =
        Logger.getLogger(ManualFlowControlClient.class.getName());
    private static final int WINDOW_SIZE = 1_000_000;

  public static void main(String[] args) throws InterruptedException {
    final CountDownLatch done = new CountDownLatch(1);

    // Create a channel and a stub
    CompressorRegistry compressorRegistry = CompressorRegistry.newEmptyInstance();
    compressorRegistry.register(Codec.Identity.NONE);
    DecompressorRegistry decompressorRegistry =
        DecompressorRegistry.emptyInstance().with(Codec.Identity.NONE, true);

    ManagedChannel channel =
        NettyChannelBuilder.forAddress("localhost", 50051)
            .keepAliveTime(5, TimeUnit.SECONDS)
            .keepAliveTimeout(30, TimeUnit.SECONDS)
            .compressorRegistry(compressorRegistry)
            .decompressorRegistry(decompressorRegistry)
            .usePlaintext()
            //.flowControlWindow(1_000_000)
            .build();
    StreamingGreeterGrpc.StreamingGreeterStub stub = StreamingGreeterGrpc.newStub(channel);

    // When using manual flow-control and back-pressure on the client, the ClientResponseObserver handles both
    // request and response streams.
    ClientResponseObserver<HelloRequest, HelloReply> clientResponseObserver =
        new ClientResponseObserver<HelloRequest, HelloReply>() {

          ClientCallStreamObserver<HelloRequest> requestStream;
          int windowSize = WINDOW_SIZE;

          @Override
          public void beforeStart(final ClientCallStreamObserver<HelloRequest> requestStream) {
            this.requestStream = requestStream;
            // Set up manual flow control for the response stream. It feels backwards to configure the response
            // stream's flow control using the request stream's observer, but this is the way it is.
            requestStream.disableAutoRequestWithInitial(WINDOW_SIZE);
            String name = String.valueOf(System.nanoTime());
            final HelloRequest request = HelloRequest.newBuilder().setName(name).build();

            // Set up a back-pressure-aware producer for the request stream. The onReadyHandler will be invoked
            // when the consuming side has enough buffer space to receive more messages.
            //
            // Messages are serialized into a transport-specific transmit buffer. Depending on the size of this buffer,
            // MANY messages may be buffered, however, they haven't yet been sent to the server. The server must call
            // request() to pull a buffered message from the client.
            //
            // Note: the onReadyHandler's invocation is serialized on the same thread pool as the incoming
            // StreamObserver's onNext(), onError(), and onComplete() handlers. Blocking the onReadyHandler will prevent
            // additional messages from being processed by the incoming StreamObserver. The onReadyHandler must return
            // in a timely manner or else message processing throughput will suffer.
            requestStream.setOnReadyHandler(new Runnable() {
              @Override
              public void run() {
                // Start generating values from where we left off on a non-gRPC thread.
                while (requestStream.isReady()) {
                  requestStream.onNext(request);
                }
              }
            });
          }

          @Override
          public void onNext(HelloReply value) {
            logger.info("<-- " + value.getMessage());
            if (--windowSize == WINDOW_SIZE / 2) {
              windowSize += WINDOW_SIZE;
              requestStream.request(WINDOW_SIZE);
            }
          }

          @Override
          public void onError(Throwable t) {
            t.printStackTrace();
            done.countDown();
          }

          @Override
          public void onCompleted() {
            logger.info("All Done");
            done.countDown();
          }
        };

    // Note: clientResponseObserver is handling both request and response stream processing.
    stub.sayHelloStreaming(clientResponseObserver);

    done.await();

    channel.shutdown();
    channel.awaitTermination(365, TimeUnit.DAYS);
  }

  private static List<String> names() {
    return Arrays.asList(
        "Sophia",
        "Jackson",
        "Emma",
        "Aiden",
        "Olivia",
        "Lucas",
        "Ava",
        "Liam",
        "Mia",
        "Noah",
        "Isabella",
        "Ethan",
        "Riley",
        "Mason",
        "Aria",
        "Caden",
        "Zoe",
        "Oliver",
        "Charlotte",
        "Elijah",
        "Lily",
        "Grayson",
        "Layla",
        "Jacob",
        "Amelia",
        "Michael",
        "Emily",
        "Benjamin",
        "Madelyn",
        "Carter",
        "Aubrey",
        "James",
        "Adalyn",
        "Jayden",
        "Madison",
        "Logan",
        "Chloe",
        "Alexander",
        "Harper",
        "Caleb",
        "Abigail",
        "Ryan",
        "Aaliyah",
        "Luke",
        "Avery",
        "Daniel",
        "Evelyn",
        "Jack",
        "Kaylee",
        "William",
        "Ella",
        "Owen",
        "Ellie",
        "Gabriel",
        "Scarlett",
        "Matthew",
        "Arianna",
        "Connor",
        "Hailey",
        "Jayce",
        "Nora",
        "Isaac",
        "Addison",
        "Sebastian",
        "Brooklyn",
        "Henry",
        "Hannah",
        "Muhammad",
        "Mila",
        "Cameron",
        "Leah",
        "Wyatt",
        "Elizabeth",
        "Dylan",
        "Sarah",
        "Nathan",
        "Eliana",
        "Nicholas",
        "Mackenzie",
        "Julian",
        "Peyton",
        "Eli",
        "Maria",
        "Levi",
        "Grace",
        "Isaiah",
        "Adeline",
        "Landon",
        "Elena",
        "David",
        "Anna",
        "Christian",
        "Victoria",
        "Andrew",
        "Camilla",
        "Brayden",
        "Lillian",
        "John",
        "Natalie",
        "Lincoln"
    );
  }
}
