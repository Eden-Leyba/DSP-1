import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class Worker {

    private static volatile boolean done = false;
    static volatile boolean terminate = false;

    private static ExecutorService parsing_thread_pool;
    private static volatile Map<Integer, Boolean> segments;
    private static final int num_parts = 4;

    private static File downloadUsingWget(String url) throws IOException, InterruptedException {

        File output = File.createTempFile("input-", ".txt");
        String outputPath = output.getAbsolutePath();

        ProcessBuilder pb = new ProcessBuilder("wget", url, "-O", outputPath);
        pb.redirectErrorStream(true);
        Process process = pb.start();

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IOException("wget failed with exit code " + exitCode);
        }

        return output;
    }

    private static List<Future<File>> runParser(File input_File_to_analyze, String requested_analysis) throws IOException {

        StanfordParser parser = new StanfordParser();
        StanfordParser.AnalysisType analysisType =
                StanfordParser.AnalysisType.valueOf(requested_analysis);

        int numSentences = parser.getNumSentences(input_File_to_analyze);

        //We divide the input file into parts and let 2 threads work on it simultaneously
        int segment_length = (int) ((numSentences + num_parts - 1) / num_parts); //round up

        segments = new LinkedHashMap<>();
        for (int i = 0; i < num_parts; i++) {
            segments.put(segment_length*i+1, false);
        }

        int segment_idx = 0;
        List<Future<File>> futures = new ArrayList<>();

        for (Map.Entry<Integer, Boolean> entry : segments.entrySet()) {

            final int   captured_segment_idx = segment_idx,
                        start_idx = entry.getKey(),
                        end_idx = start_idx + num_parts - 1;

            String output_File_Name = "analysis_" + captured_segment_idx + "_" + input_File_to_analyze.getName();
            File output_analysis_file = new File(output_File_Name);

            Future<File> output_file = parsing_thread_pool.submit(() -> {
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

    public static void main2(String[] args) throws IOException, InterruptedException {

        //int nThreads = Runtime.getRuntime().availableProcessors();
        int nThreads = 2;  // start small and see if it runs
        parsing_thread_pool = Executors.newFixedThreadPool(nThreads);

        String workers_done_queue_url = AmazonUtils.SQS.getQueueURL(Config.workers_done_queue_name);
        String workers_incoming_queue_url = AmazonUtils.SQS.getQueueURL(Config.workers_incoming_queue_name);

        while (!terminate) {
            done = false;   // reset for this task

            Message message_in_queue_msg =
                    AmazonUtils.SQS.receiveFirstMessage(workers_incoming_queue_url);
            String body = message_in_queue_msg.body();

            // Thread: extend visibility while we work
            Thread visibilityExtender = getVisibilityExtender(workers_incoming_queue_url, message_in_queue_msg);

            // Parse message
            String[] msg_split = body.split("\n");
            long local_id = Long.parseLong(msg_split[0]);
            String requested_analysis = msg_split[1];   // "POS" / "CONSTITUENCY" / "DEPENDENCY"
            String input_file_to_analyze_url = msg_split[2];

            // Download input file
            File input_File_to_analyze = downloadUsingWget(input_file_to_analyze_url);
            //String textBuffer = Files.readString(input_File_to_analyze.toPath());

            // Run parser in batches (Map-Reduce)
            List<Future<File>> parser_result = runParser(input_File_to_analyze, requested_analysis);
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

            // Upload result to S3
            long time_in_mill = System.currentTimeMillis();
            String worker_bucket_name = "dsp1-task2-" + time_in_mill; // if this is task2
            try {
                AmazonUtils.S3.createBucket(worker_bucket_name);
            } catch (S3Exception | SdkClientException e) {
                System.err.println("Worker Can't create bucket with name: " + worker_bucket_name + ", err msg: " + e.getMessage());
            }

            String analyzed_file_key = "worker-" + output_File_Name;

            try {
                AmazonUtils.S3.uploadFile(worker_bucket_name, analyzed_file_key, output_analysis_File);
            } catch (IOException e) {
                System.err.println("Worker Can't upload file: " + output_File_Name + ", IOException: " + e.getMessage());
            } catch (S3Exception e) {
                System.err.println("Worker Can't upload file: " + output_File_Name + ", AWS S3 error: " + e.awsErrorDetails().errorMessage());
            } catch (SdkClientException e) {
                System.err.println("Worker Can't upload file: " + output_File_Name + ", Client-side error: " + e.getMessage());
            }

            // Send message back to manager
            String message_output_worker =
                    local_id + "\n" +
                            input_file_to_analyze_url + "\n" +
                            worker_bucket_name + "\n" +
                            analyzed_file_key + "\n" +
                            requested_analysis;

            AmazonUtils.SQS.sendMessage(workers_done_queue_url, message_output_worker);

            // Mark done so the visibility thread stops
            done = true;

            // Optionally wait for visibility thread to finish (not strictly needed)
            try {
                visibilityExtender.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

        }
    }

    private static Thread getVisibilityExtender(String workers_incoming_queue_url, Message message_in_queue_msg) {
        Thread visibilityExtender = new Thread(() -> {
            try {
                while (!done) {

                    try {
                        Thread.sleep(30_000);  // every 30 seconds
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        System.err.println("Visibility extender interrupted");
                        break;
                    }

                    AmazonUtils.SQS.sqs.changeMessageVisibility(
                            ChangeMessageVisibilityRequest.builder()
                                    .queueUrl(workers_incoming_queue_url)
                                    .receiptHandle(message_in_queue_msg.receiptHandle())
                                    .visibilityTimeout(60) // 1 minute
                                    .build());
                }

                // Delete message from the *input* queue
                AmazonUtils.SQS.DeleteMessage(workers_incoming_queue_url, message_in_queue_msg);

            } catch (Exception e) {
                System.err.println("Error in visibility extender: " + e.getMessage());
            }
        });
        visibilityExtender.start();
        return visibilityExtender;
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        long start = System.nanoTime();
        //int nThreads = Runtime.getRuntime().availableProcessors();
        int nThreads = 2;  // start small and see if it runs
        parsing_thread_pool = Executors.newFixedThreadPool(nThreads);


        // Download input file
        File input_File_to_analyze = downloadUsingWget("https://www.gutenberg.org/files/1660/1660-0.txt");
        //String textBuffer = Files.readString(input_File_to_analyze.toPath());

        // Run parser in batches (Map-Reduce)
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
}


