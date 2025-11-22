import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.exception.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

public class AmazonUtils {
    // ONE region for the whole assignment:
    private static final Region REGION = Region.US_EAST_1;   // or whichever you use

    private static final int MB = 1024 * 1024;
    private static final int LARGE_FILE_SIZE = 100 * MB;

    // shared clients
    private static final S3Client s3 = S3Client.builder()
            .region(REGION)
            .build();

    static final SqsClient sqs = SqsClient.builder()
            .region(REGION)
            .build();


    // ===================== S3 =====================
    public static class S3 {

        public static void createBucket(String bucket) throws S3Exception, SdkClientException {
            if (REGION == Region.US_EAST_1) {
                s3.createBucket(CreateBucketRequest.builder()
                        .bucket(bucket)
                        .build());
            } else {
                s3.createBucket(CreateBucketRequest.builder()
                        .bucket(bucket)
                        .createBucketConfiguration(
                                CreateBucketConfiguration.builder()
                                        .locationConstraint(REGION.id())
                                        .build())
                        .build());
            }

            System.out.println("Created bucket: " + bucket);
        }

        public static void uploadFile(String bucketName, String key, File file)
                throws IOException, S3Exception, SdkClientException {

            if (file.length() > LARGE_FILE_SIZE) {
                multipartUpload(bucketName, key, file);
            } else {
                putObject(bucketName, key, file);
            }
        }

        public static void getSmallFile(String bucketName, String key)
                throws IOException, SdkClientException {

            try {
                GetObjectRequest request = GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build();

                ResponseBytes<GetObjectResponse> objectBytes =
                        s3.getObject(request, ResponseTransformer.toBytes());

                String text = new String(objectBytes.asByteArray());
                System.out.println("Object content:");
                System.out.println(text);

            } catch (S3Exception e) {
                System.err.println("S3 error: " + e.awsErrorDetails().errorMessage());
            }
        }

        private static void putObject(String bucketName, String key, File file)
                throws IOException, S3Exception, SdkClientException {

            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentType("text/plain")
                    .build();

            try (FileInputStream fis = new FileInputStream(file)) {
                s3.putObject(putRequest,
                        RequestBody.fromInputStream(fis, file.length()));
            }
        }

        private static void multipartUpload(String bucketName, String key, File file)
                throws IOException, S3Exception, SdkClientException {

            int partNumBytes = 5 * MB; // minimum recommended by AWS

            CreateMultipartUploadRequest createReq = CreateMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            CreateMultipartUploadResponse response = s3.createMultipartUpload(createReq);
            String uploadId = response.uploadId();
            System.out.println("UploadId: " + uploadId);

            List<CompletedPart> completedParts = new ArrayList<>();

            try (FileInputStream fis = new FileInputStream(file)) {
                byte[] buffer = new byte[partNumBytes];
                int partNumber = 1;
                int bytesRead;

                while ((bytesRead = fis.read(buffer)) != -1) {
                    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, 0, bytesRead);

                    UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .uploadId(uploadId)
                            .partNumber(partNumber)
                            .build();

                    String etag = s3.uploadPart(
                            uploadPartRequest,
                            RequestBody.fromByteBuffer(byteBuffer)
                    ).eTag();

                    completedParts.add(
                            CompletedPart.builder()
                                    .partNumber(partNumber)
                                    .eTag(etag)
                                    .build()
                    );

                    partNumber++;
                }
            }

            CompletedMultipartUpload completedMultipartUpload =
                    CompletedMultipartUpload.builder()
                            .parts(completedParts)
                            .build();

            CompleteMultipartUploadRequest completeReq =
                    CompleteMultipartUploadRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .uploadId(uploadId)
                            .multipartUpload(completedMultipartUpload)
                            .build();

            s3.completeMultipartUpload(completeReq);
        }
    }


    // ===================== SQS =====================
    public static class SQS {

        public static String getQueueURL(String queueName) {
            try {
                GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                        .queueName(queueName)
                        .build();
                return sqs.getQueueUrl(getQueueRequest).queueUrl();
            } catch (QueueDoesNotExistException e) {
                return null;
            }
        }

        public static void buildQueue(String queueName) {
            try {
                CreateQueueRequest request = CreateQueueRequest.builder()
                        .queueName(queueName)
                        .build();
                sqs.createQueue(request);
                System.out.println("Created queue: " + queueName);
            } catch (QueueNameExistsException e) {
                System.out.println("Queue already exists: " + queueName);
            }
        }

        public static String getOrCreateQueueUrl(String queueName) {
            String url = getQueueURL(queueName);
            if (url != null) return url;

            buildQueue(queueName);
            return getQueueURL(queueName);
        }

        public static void sendMessage(String queueUrl, String message) {
            SendMessageRequest sendReq = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(message)
                    .build();
            sqs.sendMessage(sendReq);
            System.out.println("Sent message: " + message);
        }

        public static String receiveFirstMessages(String queueUrl) {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

            return sqs.receiveMessage(receiveRequest).messages().get(0).body();
        }

    }
}

