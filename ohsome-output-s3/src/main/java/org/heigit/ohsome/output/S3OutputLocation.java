package org.heigit.ohsome.output;

import io.minio.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class S3OutputLocation implements OutputLocation {

    private static final Logger logger = LoggerFactory.getLogger(S3OutputLocation.class);

    private final static int MAX_RETRIES = 3;

    private final MinioClient client;
    private final String bucket;
    private final Path path;

    public S3OutputLocation(MinioClient client, String bucket, Path path) {
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
        return "%s%s/%s".formatted(S3OutputLocationProvider.protocol, bucket, path);
    }

    private interface WithRetry {
        void run(int retry) throws Exception;
    }

    private interface WithRetryFailure {
        void log(int retry, Exception e);
    }

    private void withRetry(WithRetry withRetry, WithRetryFailure failure) throws Exception {
        var exception = (Exception) null;
        for (var retry = 0; retry <= MAX_RETRIES; ) {
            try {
                withRetry.run(retry++);
                return;
            } catch (Exception e) {
                failure.log(retry, e);
                exception = e;
            }
        }
        throw exception;
    }

    @Override
    public void move(Path src, Path dest) throws Exception {
        withRetry(retry -> {
                    logger.debug("uploading file {} {} -> {}.{}", bucket, src.getFileName(), dest, retry > 0 ? " Retry %d/%d".formatted(retry, MAX_RETRIES): "");
                    client.uploadObject(UploadObjectArgs.builder()
                            .bucket(bucket)
                            .filename(src.toString())
                            .object(dest.toString())
                            .build());
                }, (retry, e) ->
                        logger.warn("Failed to upload object {} to {}.", src.getFileName(), dest, e)
        );
        Files.deleteIfExists(src);
    }

    @Override
    public void delete(Path dest) throws Exception {
        withRetry(retry -> {
                    logger.debug("deleting file {} {}.  Retry {}/{}", bucket, dest, retry, MAX_RETRIES);
                    client.removeObject(RemoveObjectArgs.builder()
                            .bucket(bucket)
                            .object(dest.toString())
                            .build());
                }, (retry, e) ->
                        logger.warn("Failed to delete object {}. Retry {}/{}.", dest, retry, MAX_RETRIES, e)
        );
    }

    @Override
    public void write(Path dest, byte[] data) throws Exception {
        withRetry(retry -> {
                    client.putObject(PutObjectArgs.builder()
                            .bucket(bucket)
                            .object(dest.toString())
                            .stream(new ByteArrayInputStream(data), data.length, -1)
                            .build());
                }, (retry, e) ->
                        logger.warn("Failed to write object {}. Retry {}/{}.", dest, retry, MAX_RETRIES, e)
        );
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
