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
import java.net.URI;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MinioOutputLocationProvider implements OutputLocationProvider {
    private static final Logger logger = LoggerFactory.getLogger(MinioOutputLocationProvider.class);

    public static final String MINIO_S3_KEY_ID = "S3_KEY_ID";
    public static final String MINIO_S3_SECRET = "S3_SECRET";

    @Override
    public boolean accepts(String path) {
        return path.startsWith("minio:") || path.startsWith("s3:");
    }

    @Override
    public OutputLocation open(String path) throws Exception {
        var parts = path.split(":", 2)[1].split(";");
        var url = URI.create(parts[0]).toURL();
        var endpoint = url.getProtocol() + "://" + url.getHost() + (url.getPort() != -1 ? ":" + url.getPort() : "");
        var pathParts = url.getPath().substring(1).split("/", 2);
        var bucket = pathParts[0];
        var minioPath = Path.of(pathParts[1]);

        logger.debug("minio client url:{} endpoint:{} bucket:{} path:{}", url, endpoint, bucket, minioPath);

        var key = System.getProperty(MINIO_S3_KEY_ID, System.getenv(MINIO_S3_KEY_ID));
        var secret = System.getProperty(MINIO_S3_SECRET, System.getenv(MINIO_S3_SECRET));

        var builder = MinioClient.builder()
                .endpoint(endpoint)
                .region("eu-central-1")
                .credentials(key, secret);
        for (var i = 1; i < parts.length; i++) {
            var param = parts[i].split(":");
            if (param.length != 2) {
                System.out.println("malformed param " + parts[i]);
                continue;
            }
            if ("region".equalsIgnoreCase(param[0])) {
                builder.region(param[1]);
            }
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
