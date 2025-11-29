import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.process.DocumentPreprocessor;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.exception.*;
import software.amazon.awssdk.services.sqs.SqsClient;
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

    private static ExecutorService parsing_thread_pool;
    private static volatile Map<Integer, Boolean> segments;
    private static int num_parts;

    private static File downloadWithJava(String urlString) throws Exception {
        System.out.println("Downloading from URL: " + urlString);

        File tempFile = File.createTempFile("web-input-", ".txt");

        try (InputStream in = new URL(urlString).openStream()) {
            Files.copy(in, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }

        System.out.println("Downloaded to: " + tempFile.getAbsolutePath());
        return tempFile;
    }

    private static List<Future<File>> runParser(File input_File_to_analyze, String requested_analysis) throws IOException {

        StanfordParser parser = new StanfordParser();
        StanfordParser.AnalysisType analysisType =
                StanfordParser.AnalysisType.valueOf(requested_analysis);

        int numSentences = parser.getNumSentences(input_File_to_analyze);

        //We divide the input file into parts and let 2 threads work on it simultaneously
        int segment_length = (int) ((numSentences + num_parts - 1) / num_parts); //round up

        System.out.println("Parsing " + numSentences + " sentences using " + num_parts + " segments");

        segments = new LinkedHashMap<>();
        for (int i = 0; i < num_parts; i++) {
            segments.put(segment_length*i+1, false);
        }

        int segment_idx = 0;
        List<Future<File>> futures = new ArrayList<>();

        for (Map.Entry<Integer, Boolean> entry : segments.entrySet()) {

            final int   captured_segment_idx = segment_idx,
                    start_idx = entry.getKey(),
                    end_idx = start_idx + segment_length - 1;

            String output_File_Name = "analysis_" + captured_segment_idx + "_" + input_File_to_analyze.getName();
            File output_analysis_file = new File(output_File_Name);

            System.out.println("Created file: " + output_File_Name);

            Future<File> output_file = parsing_thread_pool.submit(() -> {
                System.out.println("Starting Task " + captured_segment_idx);
                try (PrintWriter writer = new PrintWriter(new FileWriter(output_analysis_file))) {
                    parser.parseTextBuffer(input_File_to_analyze, analysisType, writer, start_idx, end_idx);
                } catch (IOException e) {
                    System.err.println("Parser Error in ["+start_idx+","+end_idx+"]: " + e.getMessage());
                }
                return output_analysis_file;
            });

            futures.add(output_file);

            segment_idx++;
        }

        return futures;
    }

    private static void reduceFiles(List<File> inputFiles, File outputFile) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFile))) {

            for (File input : inputFiles) {
                try (BufferedReader reader = new BufferedReader(new FileReader(input))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        writer.println(line);  // write each line to output
                    }
                }
            }

        } catch (IOException e) {
            throw new RuntimeException("Error merging files: " + e.getMessage(), e);
        }
    }

    public static void main(String[] args) throws Exception {
        long start = System.nanoTime();

        int nThreads = Runtime.getRuntime().availableProcessors();
        num_parts = nThreads;
        parsing_thread_pool = Executors.newFixedThreadPool(nThreads);

        File input_File_to_analyze = downloadWithJava("https://www.gutenberg.org/files/1660/1660-0.txt");

        List<Future<File>> parser_result = runParser(input_File_to_analyze, "POS");
        List<File> output_files = new ArrayList<>();
        for (Future<File> f : parser_result) {
            try {
                output_files.add(f.get());
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        //When we reached here, all parsing tasks are finished
        String output_File_Name = "analysis_" + input_File_to_analyze.getName();
        File output_analysis_File = new File(output_File_Name);
        reduceFiles(output_files, output_analysis_File);

        long end = System.nanoTime();
        long durationNs = end - start;
        System.out.println("Parsing execution time: " + (durationNs / 1_000_000.0) + " ms");
    }

    public static void main2(String[] args) {
        String  inputFileName   = args[0],
                outputFileName  = args[1];
        int     n               = Integer.parseInt(args[2]);

        String locals_output_queue_url;
        String locals_input_queue_url;

        //Checks if a Manager node is active on the EC2 cloud. If it is not, the application will start the
        //manager node.
        if (AmazonUtils.EC2.getNumEC2WithTagRunning(Config.instances_tag_name, Config.manager_role_value) == 1) {
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

            try {
                AmazonUtils.SQS.buildQueue(Config.locals_output_queue_name);
                AmazonUtils.SQS.buildQueue(Config.locals_input_queue_name);
                System.out.println("Local: Locals output/input success");
            } catch (SqsException | SdkClientException e) {
                System.err.println("Cannot build Locals output/input queue: " + e.getMessage());
            }
        }


        locals_output_queue_url =  AmazonUtils.SQS.getQueueURL(Config.locals_output_queue_name);
        locals_input_queue_url = AmazonUtils.SQS.getQueueURL(Config.locals_input_queue_name);

        if(locals_output_queue_url == null) {
            System.err.println("Could not get locals_output queue url!");
        }
        if(locals_input_queue_url == null) {
            System.err.println("Could not get locals_input queue url!");
        }

        //Uploads the input file to S3
        //TODO: everytime we upload a file we create a new bucket, which may be an issue
        long local_id = System.currentTimeMillis();
        String bucket_name = "dsp1-task1-" + local_id;
        System.out.println("bucket: " + bucket_name);
        try {
            AmazonUtils.S3.createBucket(bucket_name);
        } catch(S3Exception | SdkClientException e) {
            System.err.println("Could not create bucket: " + e.getMessage());
            System.exit(1);
        }

        //Upload the input file to S3
        File inputFile = new File(inputFileName);
        String key = "local-app-" + inputFileName;
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
        }
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
