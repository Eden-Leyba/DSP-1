import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.*;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.exception.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;

public class LocalApp {
    static Ec2Client ec2;
    static String manager_role_value = "manager";
    static String bucket_name;
    static Region region = Region.US_EAST_1;

    public static void RunManagerEC2Instance() throws Ec2Exception {
        String amiId = "ami-0cae6d6fe6048ca2c";
        String role = manager_role_value;

        String script = "echo 'This machine is running'"; //run the manager jar
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T1_MICRO)
                .imageId(amiId)
                .maxCount(1)
                .minCount(1)
                .keyName("dsp_lab")
                .userData(Base64.getEncoder().encodeToString(script.getBytes()))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);

        String instanceId = response.instances().get(0).instanceId();

        Tag tag = Tag.builder() //Role:manager
                .key("Role")
                .value(role)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        ec2.createTags(tagRequest);
        System.out.printf(
                "Successfully started EC2 Manager instance %s based on AMI %s\n",
                instanceId, amiId);
    }

    public static boolean is_manager_running() {
        DescribeInstancesRequest req = DescribeInstancesRequest.builder()
                .filters(
                        Filter.builder().name("tag:Role").values(manager_role_value).build(),
                        Filter.builder().name("instance-state-name").values("pending", "running").build()
                )
                .build();
        DescribeInstancesResponse response = ec2.describeInstances(req);

        for (Reservation reservation : response.reservations()) {
            for (Instance instance : reservation.instances()) {
                return true;
            }
        }

        return false;
    }

    private static String getSQSQueue(String queue_name) {
        //todo if queue exists, return its url, otherwise return null
        SqsClient sqs = SqsClient.builder().region(region).build();
        try {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queue_name)
                    .build();
            return sqs.getQueueUrl(getQueueRequest).queueUrl();
        } catch (QueueDoesNotExistException e) {
            return null;
        }
    }

    private static void BuildLocalsOutputQueue() {
        //check if there is already a queue with that name
        //if exists - delete all messages from the queue
        //if not exists - create it

        //todo
        // if getSQSQueue("LocalsOutput")!=null
        //       delete all messages from the queue (?)
        // else

        //todo
        SqsClient sqs = SqsClient.builder().region(region).build();
        String queue_name = "LocalsOutput";
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queue_name)
                    .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
        } catch (QueueNameExistsException e) {
            //If in a race condition 2 locals are trying to create the queue, just print a log msg and move on
            System.out.println("QueueNameExistsException: " + e.getMessage());
        }
    }

    public static void LocalMain(String[] args) {
        ec2 = Ec2Client.builder()
                .region(region)
                .build();

        String  inputFileName   = args[0],
                outputFileName  = args[1];
        int     n               = Integer.parseInt(args[2]);

        String queue_url;

        if (is_manager_running()) {
            System.out.println("Manager is running!");
        } else {
            try {
                RunManagerEC2Instance();
                BuildLocalsOutputQueue();
            } catch (Ec2Exception e) {
                System.out.println("Could not run EC2 Manager instance: " + e.getMessage());
                ec2.close();
                System.exit(1);
            }
        }

        queue_url = getSQSQueue("LocalsOutput");

        //Initialize AmazonUtils Object
        AmazonUtils amazonUtils = new AmazonUtils(region);

        bucket_name = "dsp1-task1-" + System.currentTimeMillis();
        //Create a bucket
        try {
            AmazonUtils.S3.createBucket(bucket_name);
        }
        catch (S3Exception e) {
            System.err.println("AWS S3 error: " + e.awsErrorDetails().errorMessage());
        }
        catch (SdkClientException e) {
            System.err.println("Client-side error: " + e.getMessage());
        }

        //Upload the input file to S3
        File inputFile = new File(inputFileName);
        String key = "local-app-" + inputFileName;
        try {
            AmazonUtils.S3.UploadFile(bucket_name, key, inputFile);
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        }
        catch (S3Exception e) {
            System.err.println("AWS S3 error: " + e.awsErrorDetails().errorMessage());
        }
        catch (SdkClientException e) {
            System.err.println("Client-side error: " + e.getMessage());
        }

        // SQS
        SqsClient sqs = SqsClient.builder().region(region).build();

        // Create LocalsOutput SQS queue
//        String queue_name = "LocalsOutput";
//        try {
//            CreateQueueRequest request = CreateQueueRequest.builder()
//                    .queueName(queue_name)
//                    .build();
//            CreateQueueResponse create_result = sqs.createQueue(request);
//        } catch (QueueNameExistsException e) {
//            System.err.println("QueueNameExistsException: " + e.getMessage());
//        }



        // send message
        String message_body = bucket_name + "\n" + key;
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queue_url)
                .messageBody(message_body)
                .build();
        sqs.sendMessage(send_msg_request);
        System.out.println("Sent message: " + message_body);

        //receive message
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queue_url)
                .build();
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();

        String received_message = messages.getFirst().body();
        System.out.println("Received message: " + received_message);
        String[] received_message_parts = received_message.split("\n");
        String received_bucket_name = received_message_parts[0];
        String received_key = received_message_parts[1];

        //Get the file from S3
        try {
            AmazonUtils.S3.GetSmallFile(received_bucket_name, received_key);
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        }
        catch (S3Exception e) {
            System.err.println("AWS S3 error: " + e.awsErrorDetails().errorMessage());
        }
        catch (SdkClientException e) {
            System.err.println("Client-side error: " + e.getMessage());
        }



        ec2.close();
    }

    public static void DeleteAllS3Buckets() {
        //Delete all s3 buckets
        S3Client s3 = S3Client.builder().region(region).build();

        ListBucketsResponse bucketsResponse = s3.listBuckets();

        for (Bucket bucket : bucketsResponse.buckets()) {
            String bucketName = bucket.name();

            try {
                // List all objects in the bucket
                ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                        .bucket(bucketName)
                        .build();

                ListObjectsV2Response listRes = s3.listObjectsV2(listReq);

                // Loop and delete each object
                for (S3Object obj : listRes.contents()) {
                    System.out.println("Deleting: " + obj.key());
                    DeleteObjectRequest deleteReq = DeleteObjectRequest.builder()
                            .bucket(bucketName)
                            .key(obj.key())
                            .build();

                    s3.deleteObject(deleteReq);
                }

                System.out.println("All objects deleted.");
            } catch (S3Exception e) {
                System.err.println(e.awsErrorDetails().errorMessage());
            }

            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket.name()).build();
            s3.deleteBucket(deleteBucketRequest);
            System.out.println("Deleted bucket: " + bucket.name());
        }

        s3.close();

    }
}
