import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.exception.*;

public class AmazonUtils {
    private static S3Client s3;
    private static final int mb = 1024 * 1024;
    private static final int LARGE_FILE_SIZE = 100 * mb;
    private static Region region;

    public AmazonUtils(Region r) {
        region = r;
        s3 = S3Client.builder().region(region).build();
    }

    public class S3 {
        public static void createBucket(String bucket) throws S3Exception, SdkClientException {
            if (region == Region.US_EAST_1) {
                s3.createBucket(CreateBucketRequest.builder()
                        .bucket(bucket)
                        .build());
            } else {
                s3.createBucket(CreateBucketRequest.builder()
                        .bucket(bucket)
                        .createBucketConfiguration(
                                CreateBucketConfiguration.builder()
                                        .locationConstraint(region.id())
                                        .build())
                        .build());
            }

            System.out.println(bucket);
        }

        public static void UploadFile(String bucketName, String key, File file) throws IOException, S3Exception, SdkClientException {
            if(file.length() > LARGE_FILE_SIZE) {
                multipartUpload(bucketName, key, file);
            }
            else {
                PutObject(bucketName, key, file);
            }
        }

        public static void GetSmallFile(String bucketName, String key) throws IOException, SdkClientException {
            try {
                GetObjectRequest request = GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build();

                ResponseBytes<?> objectBytes =
                        s3.getObject(request, ResponseTransformer.toBytes());

                byte[] data = objectBytes.asByteArray();
                String text = new String(data);

                System.out.println("Object content:");
                System.out.println(text);

            } catch (S3Exception e) {
                System.err.println("S3 error: " + e.awsErrorDetails().errorMessage());
            }
        }

        private static void PutObject(String bucketName, String key, File file) throws IOException, S3Exception, SdkClientException {
            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentType("text/plain")
                    .build();

            s3.putObject(putRequest, RequestBody.fromInputStream(new FileInputStream(file), file.length()));
        }

        private static void multipartUpload(String bucketName, String key, File file) throws IOException, S3Exception, SdkClientException {
            //minimum number of bytes in one part according to aws docs: https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
            int part_num_bytes = 5 * mb;

            // First create a multipart upload and get upload id
            CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                    .bucket(bucketName).key(key)
                    .build();
            CreateMultipartUploadResponse response = s3.createMultipartUpload(createMultipartUploadRequest);
            String uploadId = response.uploadId();
            System.out.println(uploadId);

            // Split the file into parts with part_num_bytes in each of them, and Upload all the different parts of the object
            FileInputStream fis = new FileInputStream(file);

            byte[] buffer = new byte[part_num_bytes];
            int partNumber = 1;
            int bytesRead;
            List<CompletedPart> completedParts = new ArrayList<>();

            while ((bytesRead = fis.read(buffer)) != -1) {
                ByteBuffer byteBuffer = ByteBuffer.wrap(buffer.clone(), 0, bytesRead);

                UploadPartRequest uploadPartRequest = UploadPartRequest
                        .builder()
                        .bucket(bucketName)
                        .key(key)
                        .uploadId(uploadId)
                        .partNumber(partNumber).build();

                String etag = s3.uploadPart(
                        uploadPartRequest,
                        RequestBody.fromByteBuffer(byteBuffer)
                ).eTag();

                CompletedPart part = CompletedPart
                        .builder()
                        .partNumber(partNumber)
                        .eTag(etag)
                        .build();
                completedParts.add(part);

                partNumber++;
            }

            fis.close();

            CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload
                    .builder()
                    .parts(completedParts)
                    .build();

            CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest
                    .builder()
                    .bucket(bucketName)
                    .key(key)
                    .uploadId(uploadId)
                    .multipartUpload(completedMultipartUpload).build();

            s3.completeMultipartUpload(completeMultipartUploadRequest);
        }
    }


    public class SQS {
        //todo
        // String getQueueURL(String queue_name)
        // void buildQueue(String queue_name)
        // void SendMessage(String queue_url, String message)
        // void ReceiveMessage(String queue_url)


    }

}
