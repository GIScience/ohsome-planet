package org.heigit.ohsome.output;

import io.minio.MinioClient;
import io.minio.UploadObjectArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;

public class MinioOutputLocation implements OutputLocation {

    private static final Logger logger = LoggerFactory.getLogger(MinioOutputLocation.class);

    private final static int MAX_RETRIES = 3;

    private final MinioClient client;
    private final String bucket;
    private final Path path;

    public MinioOutputLocation(MinioClient client, String bucket, Path path) {
        this.client = client;
        this.bucket = bucket;
        this.path = path;
    }

    @Override
    public Path resolve(String other) {
        return path.resolve(other);
    }

    @Override
    public void move(Path src, Path dest) throws Exception {
        var retries = 0;
        while (true) {
            try {
                client.uploadObject(UploadObjectArgs.builder()
                        .bucket(bucket)
                        .filename(src.toString())
                        .object(dest.toString())
                        .build());
                break;
            } catch (Exception e) {
                if (retries++ < MAX_RETRIES) {
                    logger.warn("Failed to upload object %s to %s. Retry %d/%d.".formatted(src, dest, retries, MAX_RETRIES),  e);
                    Thread.sleep(100);
                    continue;
                }
                throw e;
            }
        }
        Files.deleteIfExists(src);
    }


    @Override
    public void close() throws Exception {
        this.client.close();
    }
}
