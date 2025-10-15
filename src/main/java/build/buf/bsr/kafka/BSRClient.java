// Copyright 2025 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buf.bsr.kafka;

import com.google.protobuf.Descriptors;

/**
 * A client to a Buf Schema Registry, able to resolve a message descriptor for a given commit and
 * message.
 */
interface BSRClient {
  /**
   * Retrieves the message descriptor for the module commit ID and message full name.
   *
   * @param commitID Module commit ID (trimmed UUID).
   * @param messageFQN Full name of the Protobuf message.
   * @return The message descriptor for the specified message.
   * @throws ClientException Thrown if the message description can't be looked up.
   */
  Descriptors.Descriptor getMessageDescriptor(String commitID, String messageFQN)
      throws ClientException;
}
