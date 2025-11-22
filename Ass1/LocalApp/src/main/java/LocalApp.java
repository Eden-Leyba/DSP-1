import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.exception.*;

import java.io.File;
import java.io.IOException;

public class LocalApp {

    static String bucket_name;

//    private static String getSQSQueue(String queue_name) {
//        //todo if queue exists, return its url, otherwise return null
//        SqsClient sqs = SqsClient.builder().region(region).build();
//        try {
//            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
//                    .queueName(queue_name)
//                    .build();
//            return sqs.getQueueUrl(getQueueRequest).queueUrl();
//        } catch (QueueDoesNotExistException e) {
//            return null;
//        }
//    }

    public static void main(String[] args) {
        String  inputFileName   = args[0],
                outputFileName  = args[1];
        int     n               = Integer.parseInt(args[2]);

        String locals_output_queue_url;
        String locals_input_queue_url;

        //Checks if a Manager node is active on the EC2 cloud. If it is not, the application will start the
        //manager node.
        if (AmazonUtils.EC2.getNumEC2WithTagRunning(Config.instances_tag_name, Config.manager_role_value) == 1) {
            System.out.println("Manager is running!");
        } else {
            try {
                String script = "echo 'This machine is running'";
                String amiId = "ami-0cae6d6fe6048ca2c";
                AmazonUtils.EC2.RunEC2InstanceWithSpecificTag(amiId, Config.instances_tag_name, Config.manager_role_value, script);

                AmazonUtils.SQS.buildQueue(Config.locals_output_queue_name);
                AmazonUtils.SQS.buildQueue(Config.locals_input_queue_name);
            } catch (Ec2Exception e) {
                System.out.println("Could not run EC2 Manager instance: " + e.getMessage());
                AmazonUtils.EC2.CloseEc2Client();
                System.exit(1);
            }
        }

        locals_output_queue_url =  AmazonUtils.SQS.getQueueURL(Config.locals_output_queue_name);
        locals_input_queue_url = AmazonUtils.SQS.getQueueURL(Config.locals_input_queue_name);

        //Uploads the input file to S3
        //TODO: everytime we upload a file we create a new bucket, which may be an issue
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
            AmazonUtils.S3.uploadFile(bucket_name, key, inputFile);
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        }
        catch (S3Exception e) {
            System.err.println("AWS S3 error: " + e.awsErrorDetails().errorMessage());
        }
        catch (SdkClientException e) {
            System.err.println("Client-side error: " + e.getMessage());
        }

        // Sends a message to an SQS queue, stating the location of the file on S3
        String message_body = bucket_name + "\n" + key + "\n" + n;
        AmazonUtils.SQS.sendMessage(locals_output_queue_url ,message_body);
        System.out.println("Sent message: " + message_body);

        /*
        // Checks an SQS queue for a message indicating the process is done and the response (the
        //summary file) is available on S3.
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(locals_output_queue_url)
                .build();
        String received_message = AmazonUtils.SQS.receiveFirstMessage(locals_output_queue_url);

        System.out.println("Received message: " + received_message);
        String[] received_message_parts = received_message.split("\n");
        String received_bucket_name = received_message_parts[0];
        String received_key = received_message_parts[1];

        //Get the file from S3
        try {
            AmazonUtils.S3.getSmallFile(received_bucket_name, received_key);
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        }
        catch (S3Exception e) {
            System.err.println("AWS S3 error: " + e.awsErrorDetails().errorMessage());
        }
        catch (SdkClientException e) {
            System.err.println("Client-side error: " + e.getMessage());
        }
        */

        AmazonUtils.EC2.CloseEc2Client();
    }

    public static void DeleteAllS3Buckets() {
        //Delete all s3 buckets
        S3Client s3 = S3Client.builder().region(Config.region).build();

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
