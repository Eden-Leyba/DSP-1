import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import software.amazon.awssdk.core.ResponseBytes;
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
//                System.out.println("Object content:");
//                System.out.println(text);

            } catch (S3Exception e) {
                System.err.println("S3 error: " + e.awsErrorDetails().errorMessage());
            }

            return "";
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

        public static void DeleteMessage(String queueUrl, Message msg) {
            sqs.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(msg.receiptHandle())
                    .build());
        }

        public static String receiveFirstMessage(String queueUrl) {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();

            if (!messages.isEmpty()) {
                return messages.get(0).body();
            }

            return null;
        }

    }

    public static class EC2 {
        static Ec2Client ec2 = Ec2Client.builder()
                .region(Config.region)
                .build();

        public static void CloseEc2Client() {
            ec2.close();
        }

        /**
         * Returns number of ec2 instances created and launched
         * @param ami_id
         * @param instanceType
         * @param tag_name
         * @param tag_value
         * @param jar_bucket
         * @param jar_key
         * @param maxCount
         * @param minCount
         * @return
         * @throws Ec2Exception
         * @throws IOException
         * @throws InterruptedException
         */
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
            String script =
                    "#!/bin/bash\n" +
                    "mkdir /home/ec2-user/.aws \n"+
                    "sudo su\n" +
                    "yum update -y\n" +
                    "yum install -y java-23-amazon-corretto-headless awscli\n";

            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .instanceType(instanceType)
                    .imageId(ami_id)
                    .maxCount(maxCount)
                    .minCount(minCount)
                    .keyName("labsuser")
                    .userData(Base64.getEncoder().encodeToString(script.getBytes()))
                    .build();

            String[] publicIps = LaunchOneOrMoreInstancesUsingRequest(
                    runRequest, tag_name, tag_value
            );

            Thread.sleep(15_000);
            RunJarOnRunningInstance(publicIps, jar_bucket, jar_key);

            return publicIps.length;
        }

        /**
         * Returns 1 if created, 0 if not
         * @param ami_id
         * @param instanceType
         * @param tag_name
         * @param tag_value
         * @param jar_bucket
         * @param jar_key
         * @return
         * @throws Ec2Exception
         * @throws IOException
         * @throws InterruptedException
         */
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

        /**
         * Returns publicIps array of each ec2 instance launched
         * @param runInstancesRequest
         * @param tag_name
         * @param tag_value
         * @return
         * @throws Ec2Exception
         */
        private static String[] LaunchOneOrMoreInstancesUsingRequest(
                RunInstancesRequest runInstancesRequest,
                String tag_name,
                String tag_value
        ) throws Ec2Exception {
            RunInstancesResponse response = ec2.runInstances(runInstancesRequest);

            List<String> instance_ids = new ArrayList<>();
            for(Instance instance : response.instances()) {
                String instanceId = instance.instanceId();

                Tag tag = Tag.builder()
                        .key(tag_name)
                        .value(tag_value)
                        .build();

                CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                        .resources(instanceId)
                        .tags(tag)
                        .build();

                ec2.createTags(tagRequest);

                instance_ids.add(instanceId);
            }

            int num_instances = instance_ids.size();
            System.out.printf("Successfully started %d EC2 instance(s)\n", num_instances);

            String[] publicIps = new String[num_instances];
            for (int i = 0; i < num_instances; i++) {
                publicIps[i] =  ec2.describeInstances(
                                    DescribeInstancesRequest.builder()
                                        .instanceIds(instance_ids.get(i))
                                        .build()
                                )
                                .reservations().get(0)
                                .instances().get(0)
                                .publicIpAddress();
            }

            return publicIps;
        }

        private static void RunJarOnRunningInstance(
                String[] publicIps,
                String jar_bucket,
                String jar_key
        ) throws InterruptedException, IOException {
            if(publicIps.length == 0) {
                throw new IllegalArgumentException("RunJarOnRunningInstances: No public IP address specified");
            }
            //TODO: maybe do it in threads?
            for(String publicIp : publicIps) {
                ProcessBuilder scp_command = new ProcessBuilder(
                        "scp",
                        "-o", "StrictHostKeyChecking=no",
                        "-i",
                        Config.aws_folder_path + "\\labsuser.pem",
                        Config.aws_folder_path + "\\credentials",
                        "ec2-user@" + publicIp + ":/home/ec2-user/credentials"
                );
                scp_command.inheritIO().start().waitFor();

                ProcessBuilder ssh_command = new ProcessBuilder(
                        "ssh",
                        "-o", "StrictHostKeyChecking=no", //ignore the error of host is not in the hosts file
                        "-i", Config.aws_folder_path + "\\labsuser.pem",
                        "ec2-user@ec2-" + publicIp.replaceAll("\\.","-") + ".compute-1.amazonaws.com",
                        "bash << 'EOF'\n" +
                        "sudo mv /home/ec2-user/credentials /home/ec2-user/.aws/credentials\n" +
                        "aws s3 cp s3://"+ jar_bucket +"/"+ jar_key +" /home/ec2-user/app.jar\n" +
                        "cd /home/ec2-user\n" +
                        "sudo yum install -y java-23-amazon-corretto-headless \n" +
                        "java -jar app.jar > app.log \n" +
                        "EOF"
                );
                ssh_command.inheritIO().start().waitFor();
            }
        }

        //TODO: delete
        public static String RunEC2InstanceWithSpecificTag(String ami_id, String tag_name, String tag_value, String data_script) throws Ec2Exception {
//            String amiId = "ami-0cae6d6fe6048ca2c";

//            String script = "echo 'This machine is running'"; //run the manager jar
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .instanceType(InstanceType.T1_MICRO)
                    .imageId(ami_id)
                    .maxCount(1)
                    .minCount(1)
                    .keyName("labsuser")
                    .userData(Base64.getEncoder().encodeToString(data_script.getBytes()))
                    .build();

            RunInstancesResponse response = ec2.runInstances(runRequest);

            String instanceId = response.instances().get(0).instanceId();


            Tag tag = Tag.builder()
                    .key(tag_name)
                    .value(tag_value)
                    .build();

            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                    .resources(instanceId)
                    .tags(tag)
                    .build();

            ec2.createTags(tagRequest);
            System.out.printf(
                    "Successfully started EC2 Manager instance %s based on AMI %s\n",
                    instanceId, ami_id);

            String publicIp = ec2.describeInstances(
                            DescribeInstancesRequest.builder()
                                    .instanceIds(instanceId)
                                    .build()
                    ).reservations().get(0)
                    .instances().get(0)
                    .publicIpAddress();
            return publicIp;
        }

        // Return the number of EC2 instances which are running with the provided tag
        public static int getNumEC2WithTagRunning(String tag_name, String tag_value) {
            DescribeInstancesRequest req = DescribeInstancesRequest.builder()
                    .filters(
                            Filter.builder().name("tag:"+tag_name).values(tag_value).build(),
                            Filter.builder().name("instance-state-name").values("pending", "running").build()
                    )
                    .build();
            DescribeInstancesResponse response = ec2.describeInstances(req);

            int result = 0;
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    result++;
                }
            }

            return result;
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

        public static Map<String, AttributeValue> getEntry(int localId) throws IllegalArgumentException {
            GetItemRequest request = GetItemRequest.builder()
                    .tableName(table_name)
                    .key(Map.of(
                            "localId", AttributeValue.fromN(String.valueOf(localId)) // primary key
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
                int localId,
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

        public static void deleteEntry(int localId) {
            DeleteItemRequest request = DeleteItemRequest.builder()
                    .tableName(table_name)
                    .key(Map.of(
                            "local_id", AttributeValue.fromN(String.valueOf(localId))
                    ))
                    .build();

            DynamoDB.dynamo.deleteItem(request);
            System.out.println("Deleted entry for ID: " + localId);
        }

        public static void incrementNumDone(int localId) {
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

