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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Reads Buf-specific manifest entries from the JAR file containing a given class. These entries
 * allow {@link ProtoSerializer} to populate the {@value
 * ProtoDeserializer#HEADER_BUF_REGISTRY_VALUE_SCHEMA_COMMIT} Kafka header without contacting the
 * BSR.
 *
 * <p>To add these entries to the JAR containing the generated proto classes, configure
 * maven-jar-plugin in that project's POM. The commit value should come from whatever mechanism your
 * build uses to record the BSR module version (e.g. a Maven property set during code generation):
 *
 * <pre>{@code
 * <plugin>
 *   <groupId>org.apache.maven.plugins</groupId>
 *   <artifactId>maven-jar-plugin</artifactId>
 *   <configuration>
 *     <archive>
 *       <manifestEntries>
 *         <Buf-Module>buf.build/someorg/somemodule</Buf-Module>
 *         <Buf-Module-Commit>a1b2c3d4e5f6789012345678901234ab</Buf-Module-Commit>
 *         <Buf-Plugin>buf.build/protocolbuffers/java</Buf-Plugin>
 *         <Buf-Plugin-Version>v34.0.0</Buf-Plugin-Version>
 *         <Buf-Plugin-Revision>1</Buf-Plugin-Revision>
 *       </manifestEntries>
 *     </archive>
 *   </configuration>
 * </plugin>
 * }</pre>
 *
 * <p>This approach does not work with shaded JARs since shading merges manifests.
 */
final class BufManifest {

  static final String ATTRIBUTE_BUF_MODULE = "Buf-Module";
  static final String ATTRIBUTE_BUF_MODULE_COMMIT = "Buf-Module-Commit";

  private static final BufManifest EMPTY = new BufManifest("", "");
  private static final ConcurrentMap<String, BufManifest> cache = new ConcurrentHashMap<>();

  private final String module;
  private final String moduleCommit;

  private BufManifest(String module, String moduleCommit) {
    this.module = module;
    this.moduleCommit = moduleCommit;
  }

  /**
   * Returns the BufManifest for the JAR containing the given class. Returns an empty BufManifest if
   * the class is not loaded from a JAR, or the JAR has no Buf manifest entries.
   */
  static BufManifest forClass(Class<?> clazz) {
    ProtectionDomain pd = clazz.getProtectionDomain();
    if (pd == null) {
      return EMPTY;
    }
    CodeSource cs = pd.getCodeSource();
    if (cs == null) {
      return EMPTY;
    }
    URL location = cs.getLocation();
    if (location == null) {
      return EMPTY;
    }
    return cache.computeIfAbsent(location.toString(), key -> fromJarLocation(location));
  }

  /**
   * Returns a BufManifest read from the JAR at the given URL. Returns an empty BufManifest if the
   * URL is null, does not point to a JAR file, or the JAR has no Buf manifest entries.
   */
  static BufManifest fromJarLocation(URL location) {
    if (location == null || !location.toString().endsWith(".jar")) {
      return EMPTY;
    }
    try {
      URI uri = location.toURI();
      try (JarFile jar = new JarFile(new File(uri))) {
        Manifest manifest = jar.getManifest();
        if (manifest == null) {
          return EMPTY;
        }
        Attributes attrs = manifest.getMainAttributes();
        String module = nullToEmpty(attrs.getValue(ATTRIBUTE_BUF_MODULE));
        String moduleCommit = nullToEmpty(attrs.getValue(ATTRIBUTE_BUF_MODULE_COMMIT));
        if (module.isEmpty() && moduleCommit.isEmpty()) {
          return EMPTY;
        }
        return new BufManifest(module, moduleCommit);
      }
    } catch (IOException | URISyntaxException e) {
      return EMPTY;
    }
  }

  /** Returns the BSR module reference (e.g., {@code buf.build/someorg/somemodule}), or empty. */
  String getModule() {
    return module;
  }

  /** Returns the BSR module commit, or empty. */
  String getModuleCommit() {
    return moduleCommit;
  }

  private static String nullToEmpty(String value) {
    return value != null ? value : "";
  }
}
