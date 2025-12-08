package org.heigit.ohsome.output;

import io.minio.MinioClient;
import io.minio.UploadObjectArgs;
import io.minio.errors.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class MinioOutputLocation implements OutputLocation {

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
    public void move(Path src, Path dest) throws IOException {
        try {
            client.uploadObject(UploadObjectArgs.builder()
                    .bucket(bucket)
                    .filename(src.toString())
                    .object(dest.toString())
                    .build());
            Files.deleteIfExists(src);
        } catch(IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws Exception {
        this.client.close();
    }
}
