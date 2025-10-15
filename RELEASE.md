# Releasing

This document outlines how to create a release of bsr-kafka-serde-java.

1. Run the Release workflow: https://github.com/bufbuild/bsr-kafka-serde-java/actions/workflows/release.yaml
   - This workflow accepts a single optional input (release version). If unspecified, the version specified in the pom.xml `<version>` tag (minus the `-SNAPSHOT` suffix) will be the release version.
   - The release workflow will create a tag, publish the release to Maven Central, and bump the version on main to a new `-SNAPSHOT` version.
 
2. Once the release job is complete, artifacts will be available within ~30 minutes at https://repo1.maven.org/maven2/build/buf/bsr/kafka/bsr-kafka-serde.

3. Create a new release for the tag (https://github.com/bufbuild/bsr-kafka-serde-go/releases) and add release notes.

4. Publish the release.
