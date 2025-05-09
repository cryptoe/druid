/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.data.input.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.guice.ObjectMapperModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.apache.druid.common.aws.AWSClientConfig;
import org.apache.druid.common.aws.AWSCredentialsUtils;
import org.apache.druid.common.aws.AWSEndpointConfig;
import org.apache.druid.common.aws.AWSProxyConfig;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.impl.systemfield.SystemField;
import org.apache.druid.data.input.impl.systemfield.SystemFields;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.storage.s3.NoopServerSideEncryption;
import org.apache.druid.storage.s3.S3InputDataConfig;
import org.apache.druid.storage.s3.S3TransferConfig;
import org.apache.druid.storage.s3.S3Utils;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.utils.CompressionUtils;
import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.easymock.EasyMock.expectLastCall;

public class S3InputSourceTest extends InitializedNullHandlingTest
{
  private static final ObjectMapper MAPPER = createS3ObjectMapper();
  public static final AmazonS3Client S3_CLIENT = EasyMock.createMock(AmazonS3Client.class);
  private static final ClientConfiguration CLIENT_CONFIGURATION = EasyMock.createMock(ClientConfiguration.class);
  public static final ServerSideEncryptingAmazonS3.Builder SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER =
      EasyMock.createMock(ServerSideEncryptingAmazonS3.Builder.class);
  public static final AmazonS3ClientBuilder AMAZON_S3_CLIENT_BUILDER = AmazonS3Client.builder();
  public static final ServerSideEncryptingAmazonS3 SERVICE = new ServerSideEncryptingAmazonS3(
      S3_CLIENT,
      new NoopServerSideEncryption(),
      new S3TransferConfig()
  );
  public static final S3InputDataConfig INPUT_DATA_CONFIG;
  private static final int MAX_LISTING_LENGTH = 10;

  private static final List<CloudObjectLocation> EXPECTED_OBJECTS = Arrays.asList(
      new CloudObjectLocation(URI.create("s3://foo/bar/file.csv")),
      new CloudObjectLocation(URI.create("s3://bar/foo/file2.csv"))
  );

  private static final List<URI> EXPECTED_URIS = Arrays.asList(
      URI.create("s3://foo/bar/file.csv"),
      URI.create("s3://bar/foo/file2.csv")
  );

  private static final List<URI> EXPECTED_COMPRESSED_URIS = Arrays.asList(
      URI.create("s3://foo/bar/file.csv.gz"),
      URI.create("s3://bar/foo/file2.csv.gz")
  );

  private static final List<CloudObjectLocation> OBJECTS_BEFORE_GLOB = Arrays.asList(
      new CloudObjectLocation(URI.create("s3://foo/bar/file.csv")),
      new CloudObjectLocation(URI.create("s3://bar/foo/file2.csv")),
      new CloudObjectLocation(URI.create("s3://bar/foo/file3.txt"))
  );

  private static final List<URI> URIS_BEFORE_GLOB = Arrays.asList(
      URI.create("s3://foo/bar/file.csv"),
      URI.create("s3://bar/foo/file2.csv"),
      URI.create("s3://bar/foo/file3.txt")
  );

  private static final List<List<CloudObjectLocation>> EXPECTED_COORDS =
      EXPECTED_URIS.stream()
                   .map(uri -> Collections.singletonList(new CloudObjectLocation(uri)))
                   .collect(Collectors.toList());

  private static final List<URI> PREFIXES = Arrays.asList(
      URI.create("s3://foo/bar"),
      URI.create("s3://bar/foo")
  );

  private static final S3InputSourceConfig CLOUD_CONFIG_PROPERTIES = new S3InputSourceConfig(
      new DefaultPasswordProvider("myKey"), new DefaultPasswordProvider("mySecret"), null, null);
  private static final AWSEndpointConfig ENDPOINT_CONFIG = new AWSEndpointConfig();
  private static final AWSProxyConfig PROXY_CONFIG = new AWSProxyConfig();
  private static final AWSClientConfig CLIENT_CONFIG = new AWSClientConfig();

  private static final List<CloudObjectLocation> EXPECTED_LOCATION =
      ImmutableList.of(new CloudObjectLocation("foo", "bar/file.csv"));

  private static final DateTime NOW = DateTimes.nowUtc();
  private static final byte[] CONTENT =
      StringUtils.toUtf8(StringUtils.format("%d,hello,world", NOW.getMillis()));

  static {
    INPUT_DATA_CONFIG = new S3InputDataConfig();
    INPUT_DATA_CONFIG.setMaxListingLength(MAX_LISTING_LENGTH);
  }

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testGetUris()
  {
    final S3InputSource withUris = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        EXPECTED_URIS,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );

    Assert.assertEquals(
        EXPECTED_URIS,
        withUris.getUris()
    );
  }

  @Test
  public void testGetPrefixes()
  {
    final S3InputSource withPrefixes = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        null,
        PREFIXES,
        null,
        null,
        null,
        null,
        null,
        null
    );

    Assert.assertEquals(
        PREFIXES,
        withPrefixes.getPrefixes()
    );
  }

  @Test
  public void testGetObjectGlob()
  {
    final S3InputSource withUris = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        EXPECTED_URIS,
        null,
        null,
        "**.parquet",
        null,
        null,
        null,
        null
    );

    Assert.assertEquals(
        "**.parquet",
        withUris.getObjectGlob()
    );
  }

  @Test
  public void testSerdeWithUris() throws Exception
  {
    final S3InputSource withUris = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        EXPECTED_URIS,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
    final S3InputSource serdeWithUris = MAPPER.readValue(MAPPER.writeValueAsString(withUris), S3InputSource.class);
    Assert.assertEquals(withUris, serdeWithUris);
    Assert.assertEquals(Collections.emptySet(), serdeWithUris.getConfiguredSystemFields());
  }

  @Test
  public void testSerdeWithUrisAndSystemFields() throws Exception
  {
    final S3InputSource withUris = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        null,
        EXPECTED_URIS,
        null,
        null,
        null,
        new SystemFields(EnumSet.of(SystemField.URI, SystemField.BUCKET, SystemField.PATH)),
        null,
        null,
        null,
        null
    );
    final S3InputSource serdeWithUris = MAPPER.readValue(MAPPER.writeValueAsString(withUris), S3InputSource.class);
    Assert.assertEquals(withUris, serdeWithUris);
    Assert.assertEquals(
        EnumSet.of(SystemField.URI, SystemField.BUCKET, SystemField.PATH),
        serdeWithUris.getConfiguredSystemFields()
    );
  }

  @Test
  public void testSerdeWithPrefixes() throws Exception
  {
    final S3InputSource withPrefixes = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        null,
        PREFIXES,
        null,
        null,
        null,
        null,
        null,
        null
    );
    final S3InputSource serdeWithPrefixes =
        MAPPER.readValue(MAPPER.writeValueAsString(withPrefixes), S3InputSource.class);
    Assert.assertEquals(withPrefixes, serdeWithPrefixes);
  }

  @Test
  public void testSerdeWithObjects() throws Exception
  {
    final S3InputSource withPrefixes = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        null,
        null,
        EXPECTED_LOCATION,
        null,
        null,
        null,
        null,
        null
    );
    final S3InputSource serdeWithPrefixes =
        MAPPER.readValue(MAPPER.writeValueAsString(withPrefixes), S3InputSource.class);
    Assert.assertEquals(withPrefixes, serdeWithPrefixes);
  }

  @Test
  public void testSerdeWithCloudConfigPropertiesWithKeyAndSecret() throws Exception
  {
    EasyMock.reset(SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER);
    EasyMock.expect(SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER.getAmazonS3ClientBuilder())
            .andStubReturn(AMAZON_S3_CLIENT_BUILDER);
    AMAZON_S3_CLIENT_BUILDER.withClientConfiguration(CLIENT_CONFIGURATION);
    EasyMock.expect(SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER.build())
            .andReturn(SERVICE);
    EasyMock.replay(SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER);
    final S3InputSource withPrefixes = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        null,
        null,
        EXPECTED_LOCATION,
        null,
        CLOUD_CONFIG_PROPERTIES,
        null,
        null,
        null
    );
    final S3InputSource serdeWithPrefixes =
        MAPPER.readValue(MAPPER.writeValueAsString(withPrefixes), S3InputSource.class);
    // This is to force the s3ClientSupplier to initialize the ServerSideEncryptingAmazonS3
    serdeWithPrefixes.createEntity(new CloudObjectLocation("bucket", "path"));
    Assert.assertEquals(withPrefixes, serdeWithPrefixes);
    EasyMock.verify(SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER);
  }

  @Test
  public void testGetTypes()
  {
    final S3InputSource inputSource = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        EXPECTED_URIS,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
    Assert.assertEquals(Collections.singleton(S3InputSource.TYPE_KEY), inputSource.getTypes());
  }

  @Test
  public void testS3InputSourceUseEndPointClientProxy()
  {
    S3InputSourceConfig mockConfigPropertiesWithoutKeyAndSecret = EasyMock.createMock(S3InputSourceConfig.class);
    AWSEndpointConfig mockAwsEndpointConfig = EasyMock.createMock(AWSEndpointConfig.class);
    AWSClientConfig mockAwsClientConfig = EasyMock.createMock(AWSClientConfig.class);
    AWSProxyConfig mockAwsProxyConfig = EasyMock.createMock(AWSProxyConfig.class);

    EasyMock.reset(mockConfigPropertiesWithoutKeyAndSecret);
    EasyMock.reset(mockAwsEndpointConfig);
    EasyMock.reset(mockAwsClientConfig);
    EasyMock.reset(mockAwsProxyConfig);

    EasyMock.expect(mockAwsEndpointConfig.getUrl()).andStubReturn("endpoint");
    EasyMock.expect(mockAwsEndpointConfig.getSigningRegion()).andStubReturn("region");

    EasyMock.expect(mockAwsClientConfig.isDisableChunkedEncoding()).andStubReturn(false);
    EasyMock.expect(mockAwsClientConfig.isEnablePathStyleAccess()).andStubReturn(false);
    EasyMock.expect(mockAwsClientConfig.isForceGlobalBucketAccessEnabled()).andStubReturn(true);
    EasyMock.expect(mockAwsClientConfig.getProtocol()).andStubReturn("http");

    EasyMock.expect(mockAwsProxyConfig.getHost()).andStubReturn("");
    EasyMock.expect(mockAwsProxyConfig.getPort()).andStubReturn(-1);
    EasyMock.expect(mockAwsProxyConfig.getUsername()).andStubReturn("");
    EasyMock.expect(mockAwsProxyConfig.getPassword()).andStubReturn("");

    EasyMock.expect(mockConfigPropertiesWithoutKeyAndSecret.getAssumeRoleArn()).andStubReturn(null);
    EasyMock.expect(mockConfigPropertiesWithoutKeyAndSecret.isCredentialsConfigured())
            .andStubReturn(false);
    EasyMock.replay(mockConfigPropertiesWithoutKeyAndSecret);
    EasyMock.replay(mockAwsEndpointConfig);
    EasyMock.replay(mockAwsClientConfig);
    EasyMock.replay(mockAwsProxyConfig);

    EasyMock.reset(SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER);

    EasyMock.expect(SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER.getAmazonS3ClientBuilder())
            .andStubReturn(AMAZON_S3_CLIENT_BUILDER);

    AMAZON_S3_CLIENT_BUILDER.withClientConfiguration(CLIENT_CONFIGURATION);

    EasyMock.expect(SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER.build())
            .andReturn(SERVICE);
    EasyMock.replay(SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER);
    final S3InputSource withPrefixes = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        null,
        null,
        EXPECTED_LOCATION,
        null,
        mockConfigPropertiesWithoutKeyAndSecret,
        mockAwsProxyConfig,
        mockAwsEndpointConfig,
        mockAwsClientConfig
    );
    Assert.assertNotNull(withPrefixes);
    // This is to force the s3ClientSupplier to initialize the ServerSideEncryptingAmazonS3
    withPrefixes.createEntity(new CloudObjectLocation("bucket", "path"));
    EasyMock.verify(SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER);
    EasyMock.verify(mockAwsEndpointConfig);
    EasyMock.verify(mockAwsClientConfig);
    EasyMock.verify(mockAwsProxyConfig);
  }

  @Test
  public void testS3InputSourceUseDefaultPasswordWhenCloudConfigPropertiesWithoutCredential()
  {
    S3InputSourceConfig mockConfigPropertiesWithoutKeyAndSecret = EasyMock.createMock(S3InputSourceConfig.class);
    EasyMock.reset(mockConfigPropertiesWithoutKeyAndSecret);
    EasyMock.expect(mockConfigPropertiesWithoutKeyAndSecret.getAssumeRoleArn()).andStubReturn(null);
    EasyMock.expect(mockConfigPropertiesWithoutKeyAndSecret.isCredentialsConfigured())
            .andStubReturn(false);
    EasyMock.replay(mockConfigPropertiesWithoutKeyAndSecret);
    EasyMock.reset(SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER);
    EasyMock.expect(SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER.build())
            .andReturn(SERVICE);
    EasyMock.replay(SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER);
    final S3InputSource withPrefixes = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        null,
        null,
        EXPECTED_LOCATION,
        null,
        mockConfigPropertiesWithoutKeyAndSecret,
        PROXY_CONFIG,
        ENDPOINT_CONFIG,
        CLIENT_CONFIG
    );
    Assert.assertNotNull(withPrefixes);
    // This is to force the s3ClientSupplier to initialize the ServerSideEncryptingAmazonS3
    withPrefixes.createEntity(new CloudObjectLocation("bucket", "path"));
    EasyMock.verify(SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER);
    EasyMock.verify(mockConfigPropertiesWithoutKeyAndSecret);
  }

  @Test
  public void testSerdeS3ClientLazyInitializedWithCredential() throws Exception
  {
    // Amazon S3 builder should not build anything as we did not make any call that requires the S3 client
    EasyMock.reset(SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER);
    EasyMock.replay(SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER);
    final S3InputSource withPrefixes = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        null,
        null,
        EXPECTED_LOCATION,
        null,
        CLOUD_CONFIG_PROPERTIES,
        null,
        null,
        null
    );
    final S3InputSource serdeWithPrefixes =
        MAPPER.readValue(MAPPER.writeValueAsString(withPrefixes), S3InputSource.class);
    Assert.assertEquals(withPrefixes, serdeWithPrefixes);
    EasyMock.verify(SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER);
  }

  @Test
  public void testSerdeS3ClientLazyInitializedWithoutCredential() throws Exception
  {
    // Amazon S3 builder should not build anything as we did not make any call that requires the S3 client
    EasyMock.reset(SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER);
    EasyMock.replay(SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER);
    final S3InputSource withPrefixes = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        null,
        null,
        EXPECTED_LOCATION,
        null,
        null,
        null,
        null,
        null
    );
    final S3InputSource serdeWithPrefixes =
        MAPPER.readValue(MAPPER.writeValueAsString(withPrefixes), S3InputSource.class);
    Assert.assertEquals(withPrefixes, serdeWithPrefixes);
    EasyMock.verify(SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER);
  }

  @Test
  public void testSerdeWithExtraEmptyLists() throws Exception
  {
    final S3InputSource withPrefixes = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        ImmutableList.of(),
        ImmutableList.of(),
        EXPECTED_LOCATION,
        null,
        null,
        null,
        null,
        null
    );
    final S3InputSource serdeWithPrefixes =
        MAPPER.readValue(MAPPER.writeValueAsString(withPrefixes), S3InputSource.class);
    Assert.assertEquals(withPrefixes, serdeWithPrefixes);
  }

  @Test
  public void testWithNullJsonProps()
  {
    expectedException.expect(IllegalArgumentException.class);
    // constructor will explode
    new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }

  @Test
  public void testIllegalObjectsAndUris()
  {
    expectedException.expect(IllegalArgumentException.class);
    // constructor will explode
    new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        EXPECTED_URIS,
        null,
        EXPECTED_OBJECTS,
        null,
        null,
        null,
        null,
        null
    );
  }

  @Test
  public void testIllegalObjectsAndPrefixes()
  {
    expectedException.expect(IllegalArgumentException.class);
    // constructor will explode
    new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        null,
        PREFIXES,
        EXPECTED_OBJECTS,
        null,
        null,
        null,
        null,
        null
    );
  }

  @Test
  public void testIllegalUrisAndPrefixes()
  {
    expectedException.expect(IllegalArgumentException.class);
    // constructor will explode
    new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        EXPECTED_URIS,
        PREFIXES,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }

  @Test
  public void testSerdeWithInvalidArgs()
  {
    expectedException.expect(IllegalArgumentException.class);
    // constructor will explode
    new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        EXPECTED_URIS,
        PREFIXES,
        EXPECTED_LOCATION,
        null,
        null,
        null,
        null,
        null
    );
  }

  @Test
  public void testSerdeWithOtherInvalidArgs()
  {
    expectedException.expect(IllegalArgumentException.class);
    // constructor will explode
    new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        EXPECTED_URIS,
        PREFIXES,
        ImmutableList.of(),
        null,
        null,
        null,
        null,
        null
    );
  }

  @Test
  public void testSerdeWithOtherOtherInvalidArgs()
  {
    expectedException.expect(IllegalArgumentException.class);
    // constructor will explode
    new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        ImmutableList.of(),
        PREFIXES,
        EXPECTED_LOCATION,
        null,
        null,
        null,
        null,
        null
    );
  }

  @Test
  public void testWithUrisSplit()
  {
    EasyMock.reset(S3_CLIENT);
    expectGetMetadata(EXPECTED_URIS.get(0), CONTENT);
    expectGetMetadata(EXPECTED_URIS.get(1), CONTENT);
    EasyMock.replay(S3_CLIENT);

    S3InputSource inputSource = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        EXPECTED_URIS,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
        new MaxSizeSplitHintSpec(5, null)
    );

    Assert.assertEquals(EXPECTED_COORDS, splits.map(InputSplit::get).collect(Collectors.toList()));
    EasyMock.verify(S3_CLIENT);
  }

  @Test
  public void testWithUrisObjectGlob()
  {
    EasyMock.reset(S3_CLIENT);
    expectGetMetadata(EXPECTED_URIS.get(0), CONTENT);
    expectGetMetadata(EXPECTED_URIS.get(1), CONTENT);
    EasyMock.replay(S3_CLIENT);

    S3InputSource inputSource = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        URIS_BEFORE_GLOB,
        null,
        null,
        "**.csv",
        null,
        null,
        null,
        null
    );

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
        new MaxSizeSplitHintSpec(5, null)
    );

    Assert.assertEquals(EXPECTED_COORDS, splits.map(InputSplit::get).collect(Collectors.toList()));
    EasyMock.verify(S3_CLIENT);
  }

  @Test
  public void testWithObjectsGlob()
  {
    EasyMock.reset(S3_CLIENT);
    expectGetMetadata(EXPECTED_URIS.get(0), CONTENT);
    expectGetMetadata(EXPECTED_URIS.get(1), CONTENT);
    EasyMock.replay(S3_CLIENT);

    S3InputSource inputSource = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        null,
        null,
        OBJECTS_BEFORE_GLOB,
        "**.csv",
        null,
        null,
        null,
        null
    );

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
        new MaxSizeSplitHintSpec(5, null)
    );

    Assert.assertEquals(EXPECTED_COORDS, splits.map(InputSplit::get).collect(Collectors.toList()));
    EasyMock.verify(S3_CLIENT);
  }

  @Test
  public void testWithoutObjectsGlob()
  {
    EasyMock.reset(S3_CLIENT);
    expectGetMetadata(EXPECTED_URIS.get(0), CONTENT);
    expectGetMetadata(EXPECTED_URIS.get(1), CONTENT);
    EasyMock.replay(S3_CLIENT);

    S3InputSource inputSource = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        null,
        null,
        EXPECTED_OBJECTS,
        null,
        null,
        null,
        null,
        null
    );

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
        new MaxSizeSplitHintSpec(5, null)
    );

    Assert.assertEquals(EXPECTED_COORDS, splits.map(InputSplit::get).collect(Collectors.toList()));
    EasyMock.verify(S3_CLIENT);
  }

  @Test
  public void testWithPrefixesSplit()
  {
    EasyMock.reset(S3_CLIENT);
    expectListObjects(PREFIXES.get(0), ImmutableList.of(EXPECTED_URIS.get(0)), CONTENT);
    expectListObjects(PREFIXES.get(1), ImmutableList.of(EXPECTED_URIS.get(1)), CONTENT);
    EasyMock.replay(S3_CLIENT);

    S3InputSource inputSource = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        null,
        PREFIXES,
        null,
        null,
        null,
        null,
        null,
        null
    );

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
        new MaxSizeSplitHintSpec(null, 1)
    );

    Assert.assertEquals(EXPECTED_COORDS, splits.map(InputSplit::get).collect(Collectors.toList()));
    EasyMock.verify(S3_CLIENT);
  }

  @Test
  public void testGetPrefixesSplitStreamWithObjectGlob()
  {
    EasyMock.reset(S3_CLIENT);
    expectListObjects(PREFIXES.get(0), ImmutableList.of(URIS_BEFORE_GLOB.get(0)), CONTENT);
    expectListObjects(PREFIXES.get(1), ImmutableList.of(URIS_BEFORE_GLOB.get(1), URIS_BEFORE_GLOB.get(2)), CONTENT);
    EasyMock.replay(S3_CLIENT);

    S3InputSource inputSource = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        null,
        PREFIXES,
        null,
        "**.csv",
        null,
        null,
        null,
        null
    );

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
        new MaxSizeSplitHintSpec(null, 1)
    );

    Assert.assertEquals(EXPECTED_COORDS, splits.map(InputSplit::get).collect(Collectors.toList()));
    EasyMock.verify(S3_CLIENT);
  }

  @Test
  public void testCreateSplitsWithSplitHintSpecRespectingHint()
  {
    EasyMock.reset(S3_CLIENT);
    expectListObjects(PREFIXES.get(0), ImmutableList.of(EXPECTED_URIS.get(0)), CONTENT);
    expectListObjects(PREFIXES.get(1), ImmutableList.of(EXPECTED_URIS.get(1)), CONTENT);
    EasyMock.replay(S3_CLIENT);

    S3InputSource inputSource = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        null,
        PREFIXES,
        null,
        null,
        null,
        null,
        null,
        null
    );

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
        new MaxSizeSplitHintSpec(new HumanReadableBytes(CONTENT.length * 3L), null)
    );

    Assert.assertEquals(
        ImmutableList.of(EXPECTED_URIS.stream().map(CloudObjectLocation::new).collect(Collectors.toList())),
        splits.map(InputSplit::get).collect(Collectors.toList())
    );
    EasyMock.verify(S3_CLIENT);
  }

  @Test
  public void testCreateSplitsWithEmptyObjectsIteratingOnlyNonEmptyObjects()
  {
    EasyMock.reset(S3_CLIENT);
    expectListObjects(PREFIXES.get(0), ImmutableList.of(EXPECTED_URIS.get(0)), CONTENT);
    expectListObjects(PREFIXES.get(1), ImmutableList.of(EXPECTED_URIS.get(1)), new byte[0]);
    EasyMock.replay(S3_CLIENT);

    S3InputSource inputSource = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        null,
        PREFIXES,
        null,
        null,
        null,
        null,
        null,
        null
    );

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
        null
    );
    Assert.assertEquals(
        ImmutableList.of(ImmutableList.of(new CloudObjectLocation(EXPECTED_URIS.get(0)))),
        splits.map(InputSplit::get).collect(Collectors.toList())
    );
    EasyMock.verify(S3_CLIENT);
  }

  @Test
  public void testAccessDeniedWhileListingPrefix()
  {
    EasyMock.reset(S3_CLIENT);
    expectListObjects(PREFIXES.get(0), ImmutableList.of(EXPECTED_URIS.get(0)), CONTENT);
    expectListObjectsAndThrowAccessDenied(EXPECTED_URIS.get(1));
    EasyMock.replay(S3_CLIENT);

    S3InputSource inputSource = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        null,
        ImmutableList.of(PREFIXES.get(0), EXPECTED_URIS.get(1)),
        null,
        null,
        null,
        null,
        null,
        null
    );

    expectedException.expectMessage("Failed to get object summaries from S3 bucket[bar], prefix[foo/file2.csv]");
    expectedException.expectCause(
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("can't list that bucket"))
    );

    inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
        null
    ).collect(Collectors.toList());
  }

  @Test
  public void testReader() throws IOException
  {
    EasyMock.reset(S3_CLIENT);
    expectListObjects(PREFIXES.get(0), ImmutableList.of(EXPECTED_URIS.get(0)), CONTENT);
    expectListObjects(EXPECTED_URIS.get(1), ImmutableList.of(EXPECTED_URIS.get(1)), CONTENT);
    expectGetObject(EXPECTED_URIS.get(0));
    expectGetObject(EXPECTED_URIS.get(1));
    EasyMock.replay(S3_CLIENT);

    S3InputSource inputSource = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        null,
        ImmutableList.of(PREFIXES.get(0), EXPECTED_URIS.get(1)),
        null,
        null,
        null,
        null,
        null,
        null
    );

    InputRowSchema someSchema = new InputRowSchema(
        new TimestampSpec("time", "auto", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2"))),
        ColumnsFilter.all()
    );

    InputSourceReader reader = inputSource.reader(
        someSchema,
        new CsvInputFormat(ImmutableList.of("time", "dim1", "dim2"), "|", false, null, 0, null),
        temporaryFolder.newFolder()
    );

    CloseableIterator<InputRow> iterator = reader.read();

    while (iterator.hasNext()) {
      InputRow nextRow = iterator.next();
      Assert.assertEquals(NOW, nextRow.getTimestamp());
      Assert.assertEquals("hello", nextRow.getDimension("dim1").get(0));
      Assert.assertEquals("world", nextRow.getDimension("dim2").get(0));
    }

    EasyMock.verify(S3_CLIENT);
  }

  @Test
  public void testReaderRetriesOnSdkClientExceptionButNeverSucceedsThenThrows() throws Exception
  {
    EasyMock.reset(S3_CLIENT);
    expectListObjects(PREFIXES.get(0), ImmutableList.of(EXPECTED_URIS.get(0)), CONTENT);
    expectSdkClientException(EXPECTED_URIS.get(0));
    EasyMock.replay(S3_CLIENT);

    S3InputSource inputSource = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        null,
        ImmutableList.of(PREFIXES.get(0)),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        3 // only have three retries since they are slow
    );

    InputRowSchema someSchema = new InputRowSchema(
        new TimestampSpec("time", "auto", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2"))),
        ColumnsFilter.all()
    );

    InputSourceReader reader = inputSource.reader(
        someSchema,
        new CsvInputFormat(ImmutableList.of("time", "dim1", "dim2"), "|", false, null, 0, null),
        temporaryFolder.newFolder()
    );
    try (CloseableIterator<InputRow> readerIterator = reader.read()) {
      final IllegalStateException e = Assert.assertThrows(IllegalStateException.class, readerIterator::hasNext);
      MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(IOException.class));
      MatcherAssert.assertThat(e.getCause().getCause(), CoreMatchers.instanceOf(SdkClientException.class));
      MatcherAssert.assertThat(
          e.getCause().getCause().getMessage(),
          CoreMatchers.startsWith("Data read has a different length than the expected")
      );
    }

    EasyMock.verify(S3_CLIENT);
  }

  @Test
  public void testCompressedReader() throws IOException
  {
    EasyMock.reset(S3_CLIENT);
    expectListObjects(PREFIXES.get(0), ImmutableList.of(EXPECTED_COMPRESSED_URIS.get(0)), CONTENT);
    expectListObjects(EXPECTED_COMPRESSED_URIS.get(1), ImmutableList.of(EXPECTED_COMPRESSED_URIS.get(1)), CONTENT);
    expectGetObjectCompressed(EXPECTED_COMPRESSED_URIS.get(0));
    expectGetObjectCompressed(EXPECTED_COMPRESSED_URIS.get(1));
    EasyMock.replay(S3_CLIENT);

    S3InputSource inputSource = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        null,
        ImmutableList.of(PREFIXES.get(0), EXPECTED_COMPRESSED_URIS.get(1)),
        null,
        null,
        null,
        null,
        null,
        null
    );

    InputRowSchema someSchema = new InputRowSchema(
        new TimestampSpec("time", "auto", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2"))),
        ColumnsFilter.all()
    );

    InputSourceReader reader = inputSource.reader(
        someSchema,
        new CsvInputFormat(ImmutableList.of("time", "dim1", "dim2"), "|", false, null, 0, null),
        temporaryFolder.newFolder()
    );

    CloseableIterator<InputRow> iterator = reader.read();

    while (iterator.hasNext()) {
      InputRow nextRow = iterator.next();
      Assert.assertEquals(NOW, nextRow.getTimestamp());
      Assert.assertEquals("hello", nextRow.getDimension("dim1").get(0));
      Assert.assertEquals("world", nextRow.getDimension("dim2").get(0));
    }

    EasyMock.verify(S3_CLIENT);
  }

  @Test
  public void testSystemFields()
  {
    S3InputSource inputSource = new S3InputSource(
        SERVICE,
        SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        INPUT_DATA_CONFIG,
        null,
        ImmutableList.of(PREFIXES.get(0), EXPECTED_COMPRESSED_URIS.get(1)),
        null,
        null,
        null,
        new SystemFields(EnumSet.of(SystemField.URI, SystemField.BUCKET, SystemField.PATH)),
        null,
        null,
        null,
        null
    );

    Assert.assertEquals(
        EnumSet.of(SystemField.URI, SystemField.BUCKET, SystemField.PATH),
        inputSource.getConfiguredSystemFields()
    );

    final S3Entity entity = new S3Entity(null, new CloudObjectLocation("foo", "bar"), 0);

    Assert.assertEquals("s3://foo/bar", inputSource.getSystemFieldValue(entity, SystemField.URI));
    Assert.assertEquals("foo", inputSource.getSystemFieldValue(entity, SystemField.BUCKET));
    Assert.assertEquals("bar", inputSource.getSystemFieldValue(entity, SystemField.PATH));
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(S3InputSource.class)
                  .usingGetClass()
                  .withIgnoredFields("s3ClientSupplier", "inputDataConfig")
                  // maxRetries is nonfinal due to code structure, although it's effectively final
                  .suppress(Warning.NONFINAL_FIELDS)
                  .verify();
  }

  private static void expectListObjects(URI prefix, List<URI> uris, byte[] content)
  {
    final ListObjectsV2Result result = new ListObjectsV2Result();
    result.setBucketName(prefix.getAuthority());
    result.setKeyCount(uris.size());
    for (URI uri : uris) {
      final String bucket = uri.getAuthority();
      final String key = S3Utils.extractS3Key(uri);
      final S3ObjectSummary objectSummary = new S3ObjectSummary();
      objectSummary.setBucketName(bucket);
      objectSummary.setKey(key);
      objectSummary.setSize(content.length);
      result.getObjectSummaries().add(objectSummary);
    }

    EasyMock.expect(
        S3_CLIENT.listObjectsV2(matchListObjectsRequest(prefix))
    ).andReturn(result).once();
  }

  private static void expectGetMetadata(URI uri, byte[] content)
  {
    final CloudObjectLocation location = new CloudObjectLocation(uri);
    final ObjectMetadata result = new ObjectMetadata();
    result.setContentLength(content.length);

    EasyMock.expect(
        S3_CLIENT.getObjectMetadata(matchGetMetadataRequest(location.getBucket(), location.getPath()))
    ).andReturn(result).once();
  }

  private static void expectListObjectsAndThrowAccessDenied(final URI prefix)
  {
    AmazonS3Exception boom = new AmazonS3Exception("oh dang, you can't list that bucket friend");
    boom.setStatusCode(403);
    EasyMock.expect(
        S3_CLIENT.listObjectsV2(matchListObjectsRequest(prefix))
    ).andThrow(boom).once();
  }

  private static void expectGetObject(URI uri)
  {
    final String s3Bucket = uri.getAuthority();
    final String key = S3Utils.extractS3Key(uri);

    S3Object someObject = new S3Object();
    someObject.setBucketName(s3Bucket);
    someObject.setKey(key);
    someObject.setObjectContent(new ByteArrayInputStream(CONTENT));
    EasyMock.expect(S3_CLIENT.getObject(EasyMock.anyObject(GetObjectRequest.class))).andReturn(someObject).once();
  }


  // Setup mocks for invoking the resetable condition for the S3Entity
  private static void expectSdkClientException(URI uri) throws IOException
  {
    final String s3Bucket = uri.getAuthority();
    final String key = S3Utils.extractS3Key(uri);

    S3ObjectInputStream someInputStream = EasyMock.createMock(S3ObjectInputStream.class);
    EasyMock.expect(someInputStream.read(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt()))
            .andThrow(new SdkClientException("Data read has a different length than the expected")).anyTimes();
    someInputStream.close();
    expectLastCall().andVoid().anyTimes();

    S3Object someObject = EasyMock.createMock(S3Object.class);
    EasyMock.expect(someObject.getBucketName()).andReturn(s3Bucket).anyTimes();
    EasyMock.expect(someObject.getKey()).andReturn(key).anyTimes();
    EasyMock.expect(someObject.getObjectContent()).andReturn(someInputStream).anyTimes();

    EasyMock.expect(S3_CLIENT.getObject(EasyMock.anyObject(GetObjectRequest.class))).andReturn(someObject).anyTimes();

    EasyMock.replay(someObject);
    EasyMock.replay(someInputStream);
  }


  private static void expectGetObjectCompressed(URI uri) throws IOException
  {
    final String s3Bucket = uri.getAuthority();
    final String key = S3Utils.extractS3Key(uri);

    S3Object someObject = new S3Object();
    someObject.setBucketName(s3Bucket);
    someObject.setKey(key);
    ByteArrayOutputStream gzipped = new ByteArrayOutputStream();
    CompressionUtils.gzip(new ByteArrayInputStream(CONTENT), gzipped);
    someObject.setObjectContent(new ByteArrayInputStream(gzipped.toByteArray()));
    EasyMock.expect(S3_CLIENT.getObject(EasyMock.anyObject(GetObjectRequest.class))).andReturn(someObject).once();
  }

  private static ListObjectsV2Request matchListObjectsRequest(final URI prefixUri)
  {
    // Use an IArgumentMatcher to verify that the request has the correct bucket and prefix.
    EasyMock.reportMatcher(
        new IArgumentMatcher()
        {
          @Override
          public boolean matches(Object argument)
          {
            if (!(argument instanceof ListObjectsV2Request)) {
              return false;
            }

            final ListObjectsV2Request request = (ListObjectsV2Request) argument;
            return prefixUri.getAuthority().equals(request.getBucketName())
                   && S3Utils.extractS3Key(prefixUri).equals(request.getPrefix());
          }

          @Override
          public void appendTo(StringBuffer buffer)
          {
            buffer.append("<request for prefix [").append(prefixUri).append("]>");
          }
        }
    );

    return null;
  }

  private static GetObjectMetadataRequest matchGetMetadataRequest(final String bucket, final String key)
  {
    // Use an IArgumentMatcher to verify that the request has the correct bucket and key.
    EasyMock.reportMatcher(
        new IArgumentMatcher()
        {
          @Override
          public boolean matches(Object argument)
          {
            if (!(argument instanceof GetObjectMetadataRequest)) {
              return false;
            }

            final GetObjectMetadataRequest request = (GetObjectMetadataRequest) argument;
            return request.getBucketName().equals(bucket) && request.getKey().equals(key);
          }

          @Override
          public void appendTo(StringBuffer buffer)
          {
            buffer.append("<request for bucket[").append(buffer).append("] key[").append(key).append("]>");
          }
        }
    );

    return null;
  }

  public static ObjectMapper createS3ObjectMapper()
  {
    DruidModule baseModule = new TestS3Module();
    final Injector injector = Guice.createInjector(
        new ObjectMapperModule(),
        baseModule,
        new DruidModule()
        {
          @Provides
          public AWSCredentialsProvider getAWSCredentialsProvider()
          {
            return AWSCredentialsUtils.defaultAWSCredentialsProviderChain(null);
          }

          @Override
          public void configure(Binder binder)
          {
          }
        }
    );
    final ObjectMapper baseMapper = injector.getInstance(ObjectMapper.class);

    baseModule.getJacksonModules().forEach(baseMapper::registerModule);
    return baseMapper;
  }

  public static class TestS3Module implements DruidModule
  {
    @Override
    public List<? extends Module> getJacksonModules()
    {
      // Deserializer is need for AmazonS3Client even though it is injected.
      // See https://github.com/FasterXML/jackson-databind/issues/962.
      return ImmutableList.of(
          new SimpleModule()
              .addDeserializer(AmazonS3.class, new ItemDeserializer<>())
              .addDeserializer(AmazonS3ClientBuilder.class, new ItemDeserializer<>())
      );
    }

    @Override
    public void configure(Binder binder)
    {
    }

    @Provides
    public ServerSideEncryptingAmazonS3.Builder getServerSideEncryptingAmazonS3Builder()
    {
      return SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER;
    }

    @Provides
    public ServerSideEncryptingAmazonS3 getAmazonS3Client()
    {
      return SERVICE;
    }
  }

  public static class ItemDeserializer<T> extends StdDeserializer<T>
  {
    ItemDeserializer()
    {
      this(null);
    }

    ItemDeserializer(Class<?> vc)
    {
      super(vc);
    }

    @Override
    public T deserialize(JsonParser jp, DeserializationContext ctxt)
    {
      throw new UnsupportedOperationException();
    }
  }
}
