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

import build.buf.bsr.kafka.gen.bufstream.demo.v1.EmailUpdated;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BufManifestTest {

  @TempDir Path tempDir;

  @Test
  void forClass_returnsEmptyForDirectoryClasspath() {
    // Test classes are loaded from a directory, not a JAR.
    BufManifest manifest = BufManifest.forClass(EmailUpdated.class);
    Assertions.assertThat(manifest.getModule()).isNull();
    Assertions.assertThat(manifest.getModuleCommit()).isNull();
    Assertions.assertThat(manifest.getPlugin()).isNull();
    Assertions.assertThat(manifest.getPluginVersion()).isNull();
    Assertions.assertThat(manifest.getPluginRevision()).isNull();
  }

  @Test
  void forClass_returnsEmptyForJarWithoutBufEntries() {
    // Classes from third-party JARs that have no Buf manifest entries should return empty.
    BufManifest manifest = BufManifest.forClass(com.google.protobuf.Message.class);
    Assertions.assertThat(manifest.getModuleCommit()).isNull();
  }

  @Test
  void fromJarLocation_returnsEmptyForNull() {
    BufManifest manifest = BufManifest.fromJarLocation(null);
    Assertions.assertThat(manifest.getModuleCommit()).isNull();
  }

  @Test
  void fromJarLocation_returnsEmptyForNonJarUrl() throws IOException {
    // A directory URL (e.g. from an exploded classpath) should return empty.
    URL dirUrl = tempDir.toUri().toURL();
    BufManifest manifest = BufManifest.fromJarLocation(dirUrl);
    Assertions.assertThat(manifest.getModuleCommit()).isNull();
  }

  @Test
  void fromJarLocation_returnsManifestEntries() throws IOException {
    URL jarUrl =
        createJarWithBufEntries(
            tempDir,
            "buf.build/acme/petapis",
            "a1b2c3d4e5f6789012345678901234ab",
            "buf.build/protocolbuffers/java",
            "v34.0.0",
            "1");

    BufManifest manifest = BufManifest.fromJarLocation(jarUrl);
    Assertions.assertThat(manifest.getModule()).isEqualTo("buf.build/acme/petapis");
    Assertions.assertThat(manifest.getModuleCommit()).isEqualTo("a1b2c3d4e5f6789012345678901234ab");
    Assertions.assertThat(manifest.getPlugin()).isEqualTo("buf.build/protocolbuffers/java");
    Assertions.assertThat(manifest.getPluginVersion()).isEqualTo("v34.0.0");
    Assertions.assertThat(manifest.getPluginRevision()).isEqualTo("1");
  }

  @Test
  void fromJarLocation_returnsEmptyForJarWithoutBufEntries() throws IOException {
    URL jarUrl = createJarWithoutBufEntries(tempDir);

    BufManifest manifest = BufManifest.fromJarLocation(jarUrl);
    Assertions.assertThat(manifest.getModule()).isNull();
    Assertions.assertThat(manifest.getModuleCommit()).isNull();
    Assertions.assertThat(manifest.getPlugin()).isNull();
    Assertions.assertThat(manifest.getPluginVersion()).isNull();
    Assertions.assertThat(manifest.getPluginRevision()).isNull();
  }

  private static URL createJarWithBufEntries(
      Path dir,
      String module,
      String moduleCommit,
      String plugin,
      String pluginVersion,
      String pluginRevision)
      throws IOException {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().putValue(BufManifest.ATTRIBUTE_BUF_MODULE, module);
    manifest.getMainAttributes().putValue(BufManifest.ATTRIBUTE_BUF_MODULE_COMMIT, moduleCommit);
    manifest.getMainAttributes().putValue(BufManifest.ATTRIBUTE_BUF_PLUGIN, plugin);
    manifest.getMainAttributes().putValue(BufManifest.ATTRIBUTE_BUF_PLUGIN_VERSION, pluginVersion);
    manifest
        .getMainAttributes()
        .putValue(BufManifest.ATTRIBUTE_BUF_PLUGIN_REVISION, pluginRevision);
    return writeJar(dir.resolve("test-buf.jar"), manifest);
  }

  private static URL createJarWithoutBufEntries(Path dir) throws IOException {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    return writeJar(dir.resolve("test-plain.jar"), manifest);
  }

  private static URL writeJar(Path path, Manifest manifest) throws IOException {
    try (OutputStream out = Files.newOutputStream(path);
        JarOutputStream jos = new JarOutputStream(out, manifest)) {
      // Add a placeholder entry so the JAR is valid.
      jos.putNextEntry(new JarEntry("placeholder.txt"));
      jos.closeEntry();
    }
    return path.toUri().toURL();
  }
}
