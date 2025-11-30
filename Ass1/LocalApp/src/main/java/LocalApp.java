import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.process.DocumentPreprocessor;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.exception.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class LocalApp {

    private static long local_id;

    private static String locals_output_queue_url;
    private static String locals_input_queue_url;

    private static Thread managerChecker;

    private static void checkIfManagerRunning(boolean createQueues) {
        //Checks if a Manager node is active on the EC2 cloud. If it is not, the application will start the
        //manager node.
        if (AmazonUtils.EC2.getIdsEC2WithTagRunning(Config.instances_tag_name, Config.manager_role_value).size() == 1) {
            System.out.println("Manager is running!");
        }
        else
        {
            String amiId = "ami-0cae6d6fe6048ca2c";
            try {
                int launched = AmazonUtils.EC2.LaunchSingleInstance(
                        amiId,
                        InstanceType.T3_MICRO,
                        Config.instances_tag_name, Config.manager_role_value,
                        "jars-1763844625474",
                        "Manager.jar",
                        true,
                        Config.aws_folder_path
                );

                System.out.println("Launched: " + launched);
                if(launched == 0) {
                    System.err.println("Could not create Manager Instance!");
                }
            } catch (Ec2Exception | IOException | InterruptedException e) {
                System.err.println("Could not run EC2 Manager instance: " + e.getMessage());
                AmazonUtils.EC2.CloseEc2Client();
                System.exit(1);
            } catch (RuntimeException e) {
                System.err.println(e.getMessage());
            }

            if(createQueues) {
                try {
                    AmazonUtils.SQS.buildQueue(Config.locals_output_queue_name);
                    AmazonUtils.SQS.buildQueue(Config.locals_input_queue_name);
                } catch (SqsException | SdkClientException e) {
                    System.err.println("Cannot build Locals output/input queue: " + e.getMessage());
                }
            }
        }
    }

    private static void uploadInputFile(String inputFileName, String bucket_name, String key) {

        System.out.println("bucket: " + bucket_name);
        try {
            AmazonUtils.S3.createBucket(bucket_name);
        } catch(S3Exception | SdkClientException e) {
            System.err.println("Could not create bucket: " + e.getMessage());
            System.exit(1);
        }

        File inputFile = new File(inputFileName);

        try {
            AmazonUtils.S3.uploadFile(bucket_name, key, inputFile);
        } catch (IOException e) {
            System.err.println("Local App cannot upload file: \nIOException: " + e.getMessage());
        }
        catch (S3Exception e) {
            System.err.println("Local App cannot upload file: \nAWS S3 error: " + e.awsErrorDetails().errorMessage());
        }
        catch (SdkClientException e) {
            System.err.println("Local App cannot upload file: \nClient-side error: " + e.getMessage());
        }
    }

    private static void terminate() {
        String message = Config.msg_terminate_string;
        AmazonUtils.SQS.sendMessage(locals_output_queue_url ,message);

        //terminate manager(s)
        List<String> running_manager_ids = AmazonUtils.EC2.getIdsEC2WithTagRunning(Config.instances_tag_name, Config.worker_role_value);
        if(running_manager_ids.size() > 1) {
            System.err.println("Important Error! Seems like we have 2 or more managers fighting for control!");
        }
        for(String id : running_manager_ids) {
            AmazonUtils.EC2.terminateInstance(id);
        }
    }

    public static void main(String[] args) {
        if(args.length < 3) {
            System.err.println("Usage: <input_File_to_analyze> <output_analysis_file> <n> <optional terminate>");
            System.exit(1);
        }

        String  inputFileName   = args[0],
                outputFileName  = args[1];
        int     n               = Integer.parseInt(args[2]);

        String terminate = "";
        boolean do_terminate = false;
        if(args.length > 3) {
            terminate = args[3];
            if(terminate.equals(Config.args_terminate_string)) {
                do_terminate = true;
            }
            else {
                System.err.println("Terminate argument should be 'terminate' if you want to send a terminate message, or empty if not");
                System.exit(1);
            }
        }

        checkIfManagerRunning(true);
        managerChecker = new Thread(() -> {
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            checkIfManagerRunning(false);
        });

        locals_output_queue_url =  AmazonUtils.SQS.getQueueURL(Config.locals_output_queue_name);
        locals_input_queue_url = AmazonUtils.SQS.getQueueURL(Config.locals_input_queue_name);

        if(locals_output_queue_url == null) {
            System.err.println("Could not get locals_output queue url!");
        }
        if(locals_input_queue_url == null) {
            System.err.println("Could not get locals_input queue url!");
        }

        //Uploads the input file to S3
        local_id = System.currentTimeMillis();
        String bucket_name = "dsp1-task1-" + local_id;
        String key = "local-app-" + inputFileName;
        uploadInputFile(inputFileName, bucket_name, key);

        // Sends a message to an SQS queue, stating the location of the file on S3
        String message_body = bucket_name + "\n" + key + "\n" + n;
        AmazonUtils.SQS.sendMessage(locals_output_queue_url ,message_body);
        System.out.println("Sent message: " + message_body);

        // Checks an SQS queue for a message indicating the process is done and the response (the
        //summary file) is available on S3.
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(locals_input_queue_url)
                .maxNumberOfMessages(1)
                .waitTimeSeconds(20)  // long polling
                .visibilityTimeout(5) // give yourself time to check
                .build();

        //todo: Run occasional checks to see the manager is running, and if its not, launch it. (Recurring Job)
        boolean found_done_msg = false;
        while(!found_done_msg) {
            SqsClient sqs = SqsClient.builder().region(Config.region).build();
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();

            for (Message message : messages) {
                String[] msg_info = message.body().split("\n");
                long msg_local_id = Long.parseLong(msg_info[0]);
                if (msg_local_id == local_id) {
                    found_done_msg=true;

                    //Delete the message from locals_input
                    AmazonUtils.SQS.DeleteMessage(locals_input_queue_url, message);

                    String summary_bucket = msg_info[1],
                            summary_key = msg_info[2];
                    try {
                        String summary_file = AmazonUtils.S3.getSmallFile(summary_bucket, summary_key);
                        FileWriter fw = new FileWriter(outputFileName);
                        fw.write(summary_file);
                        fw.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    break;
                }
            }

            managerChecker.start();
        }

        //Here the output file is ready
        if(do_terminate)
            terminate();

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
