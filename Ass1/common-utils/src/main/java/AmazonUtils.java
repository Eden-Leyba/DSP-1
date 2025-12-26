import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
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
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.dynamodb.model.*;

public class AmazonUtils {

    private static final int MB = 1024 * 1024;
    private static final int LARGE_FILE_SIZE = 100 * MB;

    // ===================== S3 =====================
    public static class S3 {

        private static final S3Client s3 = S3Client.builder()
                .region(Config.region)
                .build();

        public static void createBucket(String bucket) throws S3Exception, SdkClientException {
            if (Config.region == Region.US_EAST_1) {
                s3.createBucket(CreateBucketRequest.builder()
                        .bucket(bucket)
                        .build());
            } else {
                s3.createBucket(CreateBucketRequest.builder()
                        .bucket(bucket)
                        .createBucketConfiguration(
                                CreateBucketConfiguration.builder()
                                        .locationConstraint(Config.region.id())
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

        public static void appendToS3File(String bucket, String key, String text) {
            String existing = "";
            try {
                existing = s3.getObjectAsBytes(GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build()).asUtf8String();
            } catch (Exception e) {}

            String newContent = existing + text;

            s3.putObject(PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .build(),
                    RequestBody.fromString(newContent));
        }

        public static String getFileUrl(String bucket, String key) {

            S3Presigner presigner = S3Presigner.builder()
                    .region(Config.region)
                    .build();

            PresignedGetObjectRequest req = presigner.presignGetObject(
                    GetObjectPresignRequest.builder()
                            .signatureDuration(Duration.ofHours(1))
                            .getObjectRequest(GetObjectRequest.builder()
                                    .bucket(bucket)
                                    .key(key)
                                    .build())
                            .build()
            );

            return req.url().toString();
        }

        public static String getSmallFile(String bucketName, String key)
                throws IOException, SdkClientException {

            try {
                GetObjectRequest request = GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build();

                ResponseBytes<GetObjectResponse> objectBytes =
                        s3.getObject(request, ResponseTransformer.toBytes());

                return new String(objectBytes.asByteArray());

            } catch (S3Exception e) {
                System.err.println("S3 error: " + e.awsErrorDetails().errorMessage());
            }

            return "";
        }

        public static InputStream getFileStream(String bucketName, String key) throws S3Exception, SdkClientException {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            ResponseInputStream<GetObjectResponse> s3Stream = s3.getObject(request);

            return s3Stream;
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
        static final SqsClient sqs = SqsClient.builder()
                .region(Config.region)
                .build();

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

        public static void buildQueue(String queueName) throws SqsException, SdkClientException{
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

        public static void sendMessage(String queueUrl, String message) throws SqsException, SdkClientException{
            SendMessageRequest sendReq = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(message)
                    .build();
            sqs.sendMessage(sendReq);
            System.out.println("Sent message: " + message.replaceAll("\n", ";"));
        }

        public static void DeleteMessage(String queueUrl, Message msg) throws SqsException, SdkClientException{
            sqs.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(msg.receiptHandle())
                    .build());
        }

        public static Message receiveFirstMessage(String queueUrl) {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();

            if (!messages.isEmpty()) {
                return messages.getFirst();
            }

            return null;
        }

        public static int getApproxMessagesCount(String queueUrl) {
            GetQueueAttributesResponse resp = sqs.getQueueAttributes(
                    GetQueueAttributesRequest.builder()
                            .queueUrl(queueUrl)
                            .attributeNames(
                                    QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                                    QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE
                            )
                            .build()
            );

            String count = resp.attributes().get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES);
            return Integer.parseInt(count);
        }

        public static void deleteAllMessages(String queueUrl) {
            try {
                PurgeQueueRequest request = PurgeQueueRequest.builder()
                        .queueUrl(queueUrl)
                        .build();

                sqs.purgeQueue(request);
                System.out.println("Purge requested for queue: " + queueUrl);
            } catch (SqsException e) {
                System.err.println(e.awsErrorDetails().errorMessage());
                throw e;
            }
        }

    }

    public static class EC2 {
        static Ec2Client ec2 = Ec2Client.builder()
                .region(Config.region)
                .build();

        public static void CloseEc2Client() {
            ec2.close();
        }


        public static String getInstanceId() throws Exception {
            HttpClient client = HttpClient.newHttpClient();

            // 1. Get IMDSv2 token
            HttpRequest tokenRequest = HttpRequest.newBuilder()
                    .uri(URI.create("http://169.254.169.254/latest/api/token"))
                    .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
                    .method("PUT", HttpRequest.BodyPublishers.noBody())
                    .build();

            String token = client.send(tokenRequest, HttpResponse.BodyHandlers.ofString()).body();

            // 2. Use the token to fetch the instance ID
            HttpRequest idRequest = HttpRequest.newBuilder()
                    .uri(URI.create("http://169.254.169.254/latest/meta-data/instance-id"))
                    .header("X-aws-ec2-metadata-token", token)
                    .GET()
                    .build();

            return client.send(idRequest, HttpResponse.BodyHandlers.ofString()).body();
        }

        public static void terminateMyself() throws Exception {
            String instanceId = getInstanceId();
            System.out.println("Terminating instance: " + instanceId);

            TerminateInstancesRequest req = TerminateInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();

            ec2.terminateInstances(req);
            System.out.println("TerminateInstances request sent.");
        }

        public static int LaunchMultipleInstances(
                String ami_id,
                InstanceType instanceType,
                String tag_name,
                String tag_value,
                String jar_bucket,
                String jar_key,
                int maxCount,
                int minCount
        ) throws Ec2Exception, IOException, InterruptedException{
            int vcpus = 1;
            try {
                vcpus = getVCpus(instanceType);
            } catch (RuntimeException _) {}
            String info_config_txt = "vcpus=" + vcpus + "\n";

            String user_data_script = "#!/bin/bash -xe\n" +
                    "sudo yum update -y\n" +
                    "sudo yum install -y java-25-amazon-corretto-headless awscli\n" +
                    "echo '" + info_config_txt + "' >> /home/ec2-user/"+ Config.config_file_name +"\n" +
                    "aws s3 cp s3://"+ jar_bucket +"/"+ jar_key +" /home/ec2-user/file.jar\n"+
                    "cd /home/ec2-user\n"+
                    "nohup java -Xmx2g -Xms1g -jar file.jar > app.log 2>&1 &\n";

            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .instanceType(instanceType)
                    .imageId(ami_id)
                    .maxCount(maxCount)
                    .minCount(minCount)
                    .userData(Base64.getEncoder().encodeToString(user_data_script.getBytes()))
                    .keyName("labsuser")
                    .tagSpecifications(TagSpecification.builder().resourceType(ResourceType.INSTANCE).tags(Tag.builder().key(tag_name).value(tag_value).build()).build())
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                    .build();

            RunInstancesResponse response = ec2.runInstances(runRequest);
            return response.instances().size();
        }

        public static int LaunchSingleInstance(
                String ami_id,
                InstanceType instanceType,
                String tag_name,
                String tag_value,
                String jar_bucket,
                String jar_key
        ) throws Ec2Exception, IOException, InterruptedException {
            return LaunchMultipleInstances(ami_id, instanceType, tag_name, tag_value, jar_bucket, jar_key, 1, 1);
        }

        // Return the number of EC2 instances which are running with the provided tag
        public static List<String> getIdsEC2WithTagRunning(String tag_name, String tag_value) {
            DescribeInstancesRequest req = DescribeInstancesRequest.builder()
                    .filters(
                            Filter.builder().name("tag:"+tag_name).values(tag_value).build(),
                            Filter.builder().name("instance-state-name").values("pending", "running").build()
                    )
                    .build();
            DescribeInstancesResponse response = ec2.describeInstances(req);

            List<String> result = new ArrayList<>();
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    result.add(instance.instanceId());
                }
            }

            return result;
        }

        public static void terminateInstance(String instanceId) {
            TerminateInstancesRequest req = TerminateInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();

            ec2.terminateInstances(req);

            System.out.println("Terminate signal sent to instance: " + instanceId);
        }

        public static int getVCpus(InstanceType instanceType) throws RuntimeException {

            DescribeInstanceTypesRequest req = DescribeInstanceTypesRequest.builder()
                    .instanceTypes(instanceType)
                    .build();

            DescribeInstanceTypesResponse res = ec2.describeInstanceTypes(req);

            if (res.instanceTypes().isEmpty()) {
                throw new RuntimeException("getVCpus(): InstanceType not found: " + instanceType);
            }

            InstanceTypeInfo info = res.instanceTypes().get(0);

            return info.vCpuInfo().defaultVCpus();
        }

    }

    public static class DynamoDB {
        private static String table_name;

        private static final DynamoDbClient dynamo = DynamoDbClient.builder()
                .region(Config.region)
                .build();

        public static void SetTableName(String name) {
            table_name = name;
        }

        public static Map<String, AttributeValue> getEntry(long localId) throws IllegalArgumentException {
            GetItemRequest request = GetItemRequest.builder()
                    .tableName(table_name)
                    .key(Map.of(
                            "local_id", AttributeValue.fromN(String.valueOf(localId)) // primary key
                    ))
                    .build();

            GetItemResponse response = dynamo.getItem(request);

            if (response.hasItem()) {
                return response.item();
            } else {
                throw new IllegalArgumentException("DynamoDB Error: No Local with locals_id: "+ localId);
            }
        }

        public static void createEntry(
                long localId,
                int numDone,
                int numUrls,
                String bucketName,
                String keyName)
        {
            PutItemRequest request = PutItemRequest.builder()
                    .tableName(table_name)
                    .item(Map.of(
                            "local_id", AttributeValue.fromN(String.valueOf(localId)),
                            "num_done", AttributeValue.fromN(String.valueOf(numDone)),
                            "num_urls", AttributeValue.fromN(String.valueOf(numUrls)),
                            "output_summary_s3_bucketname", AttributeValue.fromS(bucketName),
                            "output_summary_s3_key", AttributeValue.fromS(keyName)
                    ))
                    .build();

            dynamo.putItem(request);
            System.out.println("Created entry for ID: " + localId);
        }

        public static void deleteEntry(long localId) {
            DeleteItemRequest request = DeleteItemRequest.builder()
                    .tableName(table_name)
                    .key(Map.of(
                            "local_id", AttributeValue.fromN(String.valueOf(localId))
                    ))
                    .build();

            DynamoDB.dynamo.deleteItem(request);
            System.out.println("Deleted entry for ID: " + localId);
        }

        public static void incrementNumDone(long localId) {
            UpdateItemRequest request = UpdateItemRequest.builder()
                    .tableName(table_name)
                    .key(Map.of(
                            "local_id", AttributeValue.fromN(String.valueOf(localId))
                    ))
                    .updateExpression("SET num_done = num_done + :inc")
                    .expressionAttributeValues(Map.of(
                            ":inc", AttributeValue.fromN(String.valueOf(1))
                    ))
                    .conditionExpression("attribute_exists(local_id)")
                    .build();

            dynamo.updateItem(request);
            System.out.println("Incremented num_done for " + localId);
        }
    }
}