package org.heigit.ohsome.output;

import io.minio.MinioClient;
import io.minio.messages.Bucket;

import java.net.URI;
import java.nio.file.Path;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MinioOutputLocationProvider implements OutputLocationProvider {
    public static final String MINIO_S3_KEY_ID = "MINIO_S3_KEY_ID";
    public static final String MINIO_S3_SECRET = "MINIO_S3_SECRET";

    @Override
    public boolean accepts(String path) {
        return path.toLowerCase().startsWith("minio:");
    }

    @Override
    public OutputLocation open(String path) throws Exception {
        // minio:https://hot.storage.heigit.org/heigit-ohsome-planet/data/global/2025-12-03/contributions;region:eu-central-1
        var parts = path.substring(6).split(";");
        var url = URI.create(parts[0]).toURL();
        var endpoint = url.getProtocol() + "://" + url.getHost();
        var pathParts = url.getPath().substring(1).split("/", 2);
        var bucket = pathParts[0];
        var minioPath = Path.of(pathParts[1]);

        var key = System.getProperty(MINIO_S3_KEY_ID, System.getenv(MINIO_S3_KEY_ID));
        var secret = System.getProperty(MINIO_S3_SECRET, System.getProperty(MINIO_S3_SECRET));

        var builder = MinioClient.builder()
                .endpoint(endpoint)
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

        var client = (MinioClient)  null;
        try {
            client = builder.build();
            var buckets = client.listBuckets().stream().collect(Collectors.toMap(Bucket::name, Function.identity()));
            if (!buckets.containsKey(bucket)){
                System.out.println("bucket " + bucket + " does not exist");
                throw new Exception("bucket " + bucket + " does not exist");
            }
            return new MinioOutputLocation(client, bucket, minioPath);
        } catch (Exception e) {
            if (client != null) {
                client.close();
            }
            throw e;
        }
    }
}
