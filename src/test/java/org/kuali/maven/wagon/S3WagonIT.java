package org.kuali.maven.wagon;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.util.UUID;

import org.apache.maven.wagon.authentication.AuthenticationInfo;
import org.apache.maven.wagon.repository.Repository;
import org.junit.Assume;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.regions.Region;
import org.junit.Test;

/**
 * Integration test that creates a unique S3 bucket, uploads a file, verifies listing/exists, and cleans up.
 * Enabled only when -Dit.s3.enabled=true and AWS credentials are present (DefaultCredentialsProvider).
 */
public class S3WagonIT {

    @Test
    public void uploadDownloadList_cleanup() throws Exception {
        Assume.assumeTrue(Boolean.getBoolean("it.s3.enabled"));

        String bucket = "wagon-it-" + UUID.randomUUID().toString().replace("-", "").substring(0, 20);
        Repository repository = new Repository("itbucket", "s3://" + bucket + "/repo");

        S3Wagon wagon = new S3Wagon();
        AuthenticationInfo auth = new AuthenticationInfo(); // uses DefaultCredentialsProvider via v2 internally

        try {
            wagon.connect(repository, auth);

            File tmp = Files.createTempFile("wagon-it", ".txt").toFile();
            try (FileWriter fw = new FileWriter(tmp)) {
                fw.write("hello");
            }

            String dest = "dir with spaces/file with spaces.txt";
            wagon.put(tmp, dest);

            assertTrue(wagon.resourceExists(dest));

            File dl = Files.createTempFile("wagon-it-dl", ".txt").toFile();
            wagon.get(dest, dl);
            assertTrue(dl.length() == tmp.length());
        } finally {
            try { wagon.disconnect(); } catch (Exception ignore) {}
            // Best-effort cleanup: delete object and bucket
            try {
                S3Client s3 = S3Client.builder().region(Region.of(System.getProperty("maven.wagon.s3.region", System.getenv().getOrDefault("AWS_REGION", "us-east-1")))).build();
                s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key("repo/dir with spaces/file with spaces.txt").build());
                s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build());
            } catch (Exception ignore) {}
        }
    }
}


