package org.heigit.ohsome.output;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.errors.*;
import io.minio.messages.Bucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MinioOutputLocationProvider implements OutputLocationProvider {
    private static final Logger logger = LoggerFactory.getLogger(MinioOutputLocationProvider.class);

    public static final String MINIO_S3_KEY_ID = "S3_KEY_ID";
    public static final String MINIO_S3_SECRET = "S3_SECRET";
    public static final String MINIO_S3_ENDPOINT = "S3_ENDPOINT";
    public static final String MINIO_S3_REGION = "S3_REGION";

    static final String protocol = "s3://";

    @Override
    public boolean accepts(String path) {
        return path.startsWith(protocol);
    }

    @Override
    public OutputLocation open(String path) throws Exception {
        var bucketPath = path.substring(protocol.length()).split("/", 2);
        var bucket = bucketPath[0];
        var minioPath = Path.of(bucketPath[1]);


        var endpoint = System.getProperty(MINIO_S3_ENDPOINT, System.getenv(MINIO_S3_ENDPOINT));
        var region = System.getProperty(MINIO_S3_REGION, System.getenv(MINIO_S3_REGION));
        var key = System.getProperty(MINIO_S3_KEY_ID, System.getenv(MINIO_S3_KEY_ID));
        var secret = System.getProperty(MINIO_S3_SECRET, System.getenv(MINIO_S3_SECRET));
        logger.debug("s3 client endpoint:{} bucket:{} path:{}", endpoint, bucket, minioPath);

        var builder = MinioClient.builder()
                .endpoint(endpoint)
                .region("eu-central-1")
                .credentials(key, secret);
        if (region != null) {
            builder.region(region);
        }

        var client = (MinioClient) null;
        try {
            client = builder.build();
            var buckets = client.listBuckets().stream().collect(Collectors.toMap(Bucket::name, Function.identity()));
            if (!buckets.containsKey(bucket)){
                logger.error("bucket {} does not exist", bucket);
                throw new Exception("bucket " + bucket + " does not exist");
            }
            checkReadWritePermissions(minioPath, client, bucket);
            return new MinioOutputLocation(client, bucket, minioPath);
        } catch (Exception e) {
            if (client != null) {
                client.close();
            }
            throw e;
        }
    }

    private static void checkReadWritePermissions(Path minioPath, MinioClient client, String bucket) throws ErrorResponseException, InsufficientDataException, InternalException, InvalidKeyException, InvalidResponseException, IOException, NoSuchAlgorithmException, ServerException, XmlParserException {
        var probeData = "ohsome-planet".getBytes();
        var probe = minioPath.resolve("probe/probe.txt").toString();
        client.putObject(PutObjectArgs.builder()
                .bucket(bucket)
                .object(probe)
                .stream(new ByteArrayInputStream(probeData), probeData.length, -1)
                .build());
        probeData = client.getObject(GetObjectArgs.builder()
                        .bucket(bucket)
                        .object(probe)
                .build()).readAllBytes();
        client.removeObject(RemoveObjectArgs.builder()
                        .bucket(bucket)
                        .object(probe)
                .build());
        if (!"ohsome-planet".equals(new String(probeData))) {
            logger.error("could not read back probeData {} {} [{}]", bucket, probe, new String(probeData));
            throw new IOException();
        }

    }
}
