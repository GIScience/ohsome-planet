package org.heigit.ohsome.output;

import io.minio.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
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
    public String location(Path path) {
        return "%s%s/%s".formatted(MinioOutputLocationProvider.protocol, bucket, path);
    }

    @Override
    public void move(Path src, Path dest) throws Exception {
        var retries = 0;
        while (true) {
            try {
                logger.debug("uploading file {} {} -> {}", bucket, src, dest);
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
    public void delete(Path dest) throws Exception {
        client.removeObject(RemoveObjectArgs.builder()
                        .bucket(bucket)
                        .object(dest.toString())
                .build());
    }

    @Override
    public void write(Path dest, byte[] data) throws Exception {
        client.putObject(PutObjectArgs.builder()
                        .bucket(bucket)
                        .object(dest.toString())
                        .stream(new ByteArrayInputStream(data), data.length, -1)
                .build());
    }

    @Override
    public InputStream read(Path dest) throws Exception {
        return client.getObject(GetObjectArgs.builder()
                        .bucket(bucket)
                        .object(dest.toString())
                .build());
    }

    @Override
    public boolean exists() {
        return client.listObjects(ListObjectsArgs.builder().bucket(bucket).prefix(path.toString()).build()).iterator().hasNext();
    }

    @Override
    public void close() throws Exception {
        this.client.close();
    }
}
