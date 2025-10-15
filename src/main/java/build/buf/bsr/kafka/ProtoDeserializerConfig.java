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

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

/**
 * ProtoDeserializerConfig provides the configuration settings which can be used to configure {@link
 * ProtoDeserializer}.
 */
public final class ProtoDeserializerConfig extends AbstractConfig {

  /** The configuration property for the BSR hostname where schemas are resolved. */
  public static final String BSR_HOST_CONFIG = "bsr.host";

  private static final String BSR_HOST_DEFAULT = "";
  private static final String BSR_HOST_DOC = "Specify the hostname of the BSR";

  /**
   * An optional BSR API token to use to authenticate requests. This is required to access private
   * modules or bypass lower rate limits for unauthenticated requests.
   */
  public static final String BSR_TOKEN_CONFIG = "bsr.token";

  private static final String BSR_TOKEN_DEFAULT = "";
  private static final String BSR_TOKEN_DOC = "Specify the BSR API token";

  /**
   * A Protobuf generated class name. Used in place of {@link com.google.protobuf.DynamicMessage}
   * for better performance. The class name must define a static parseFrom method which takes a
   * byte[] argument.
   */
  public static final String VALUE_TYPE_CONFIG = "value.type";

  private static final String VALUE_TYPE_DOC =
      "A Protocol buffers class to deserialize values into";

  private static final ConfigDef CONFIG_DEF;

  static {
    CONFIG_DEF =
        new ConfigDef()
            .define(
                BSR_HOST_CONFIG,
                ConfigDef.Type.STRING,
                BSR_HOST_DEFAULT,
                ConfigDef.Importance.HIGH,
                BSR_HOST_DOC)
            .define(
                BSR_TOKEN_CONFIG,
                ConfigDef.Type.STRING,
                BSR_TOKEN_DEFAULT,
                ConfigDef.Importance.HIGH,
                BSR_TOKEN_DOC)
            .define(
                VALUE_TYPE_CONFIG,
                ConfigDef.Type.CLASS,
                Object.class,
                ConfigDef.Importance.MEDIUM,
                VALUE_TYPE_DOC);
  }

  /**
   * Creates a new config from the given properties.
   *
   * @param props Map of configuration properties.
   */
  public ProtoDeserializerConfig(Map<?, ?> props) {
    super(CONFIG_DEF, props);
  }
}
