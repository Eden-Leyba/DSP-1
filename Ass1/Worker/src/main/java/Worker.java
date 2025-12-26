import edu.stanford.nlp.ling.HasWord;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class Worker {

    private static volatile boolean msg_visible_to_other_workers = false;
    static volatile boolean terminate = false;

    private static ExecutorService parsing_thread_pool;
    private static volatile int[] segments;

    private static Map<String, String> config_file_lines_map;

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

    private static List<Future<File>> runParser(int num_parts, File input_File_to_analyze, String requested_analysis) throws IOException {

        StanfordParser parser = new StanfordParser();
        StanfordParser.AnalysisType analysisType =
                StanfordParser.AnalysisType.valueOf(requested_analysis);

        int numSentences = parser.getNumSentences(input_File_to_analyze);

        //We divide the input file into parts and let 2 threads work on it simultaneously
        int segment_length = (int) ((numSentences + num_parts - 1) / num_parts); //round up

        segments = new int[num_parts];
        for (int i = 0; i < num_parts; i++) {
            segments[i] = segment_length*i+1;
        }

        int segment_idx = 0;
        List<Future<File>> futures = new ArrayList<>();

        List<List<HasWord>> sentences = parser.initTokenizer(input_File_to_analyze);
        for (int segment_start_idx : segments) {

            final int   captured_segment_idx = segment_idx,
                        end_idx = segment_start_idx + segment_length - 1;

            String output_File_Name = "analysis_" + captured_segment_idx + "_" + input_File_to_analyze.getName();
            File output_analysis_file = new File(output_File_Name);

            Future<File> output_file = parsing_thread_pool.submit(() -> {
                StanfordParser parser_per_thread = new StanfordParser();
                System.out.println("Parsing from " + segment_start_idx + " to " + end_idx);

                try (PrintWriter writer = new PrintWriter(new FileWriter(output_analysis_file))) {
                    parser_per_thread.parseTextBuffer(sentences, analysisType, writer, segment_start_idx, end_idx);
                } catch (IOException e) {
                    System.err.println("Parser Error in ["+segment_start_idx+","+end_idx+"]: " + e.getMessage());
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

    private static void readConfigFile() throws IOException {
        List<String> config_file_lines = Files.readAllLines(Paths.get(Config.config_file_name));
        config_file_lines_map = new HashMap<>();
        for(String line : config_file_lines) {
            if(!line.contains("=")) {
                continue;
            }
            String key = line.split("=")[0];
            String value = line.split("=")[1];
            config_file_lines_map.put(key, value);
        }
    }

    private static String readConfigValue(String key) {
        return config_file_lines_map.get(key);
    }

    private static File runParserWrapper(int nThreads, File inputFileToAnalyze, String analysisType, String outputFileName) throws IOException, InterruptedException {
        //run parser in batches
        List<Future<File>> parser_result = runParser(nThreads*2, inputFileToAnalyze, analysisType);
        List<File> output_files = new ArrayList<>();
        for (Future<File> f : parser_result) {
            try {
                output_files.add(f.get());
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        //When we reached here, all parsing tasks are finished
        File outputAnalysisFile = new File(outputFileName);
        reduceFiles(output_files, outputAnalysisFile);

        return outputAnalysisFile;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String workers_done_queue_url = AmazonUtils.SQS.getQueueURL(Config.workers_done_queue_name);
        String workers_incoming_queue_url = AmazonUtils.SQS.getQueueURL(Config.workers_incoming_queue_name);

        readConfigFile();

        while (!terminate) {
            msg_visible_to_other_workers = false;

            Message message_in_queue_msg =
                    AmazonUtils.SQS.receiveFirstMessage(workers_incoming_queue_url);
            if(message_in_queue_msg == null) {
                break;
            }

            String msg_body = message_in_queue_msg.body();

            // Thread: extend visibility while we work
            Thread visibilityExtender = getVisibilityExtender(workers_incoming_queue_url, message_in_queue_msg);

            if(msg_body.equals(Config.msg_terminate_string)) {
                try {
                    AmazonUtils.EC2.terminateMyself();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                break;
            }

            // In case of job msg, parse message
            String[] msg_split = msg_body.split("\n");
            long local_id = Long.parseLong(msg_split[0]);
            String requested_analysis = msg_split[1];   // "POS" / "CONSTITUENCY" / "DEPENDENCY"
            String input_file_to_analyze_url = msg_split[2];

            // Download input file
            boolean download_succeeded = false;
            String outputFileName = "";
            File inputFileToAnalyze = null;
            try {
                inputFileToAnalyze = downloadUsingWget(input_file_to_analyze_url);
                outputFileName = "analysis_" + inputFileToAnalyze.getName();
                download_succeeded = true;
            } catch (Exception e) {
                // Send message back to manager
                System.err.println("Error downloading file: " + e.getMessage());
                String message_output_worker =
                        local_id + "\n" +
                                input_file_to_analyze_url + "\n" +
                                "ERROR" + "\n" +
                                "ERROR" + "\n" +
                                requested_analysis + "\n" +
                                e.getMessage();

                AmazonUtils.SQS.sendMessage(workers_done_queue_url, message_output_worker);
                // Mark done so the visibility thread stops
                msg_visible_to_other_workers = true;
            }

            if(!download_succeeded)
                continue;

            //init parsing thread pool
            int nThreads = Integer.parseInt(readConfigValue("vcpus"));
            parsing_thread_pool = Executors.newFixedThreadPool(nThreads);

            System.out.println("Parsing: " + inputFileToAnalyze + " using " + nThreads + " threads");

            File output_analysis_file = null;
            try {
                 output_analysis_file = runParserWrapper(nThreads, inputFileToAnalyze, requested_analysis, outputFileName);
            } catch(IOException | InterruptedException e) {
                System.err.println("Error in ["+inputFileToAnalyze.getName()+"]: " + e.getMessage());
            }

            // Upload result to S3
            long time_in_mill = System.currentTimeMillis();
            String worker_bucket_name = "dsp1-task1-" + time_in_mill;
            try {
                AmazonUtils.S3.createBucket(worker_bucket_name);
            } catch (S3Exception | SdkClientException e) {
                System.err.println("Worker Can't create bucket with name: " + worker_bucket_name + ", err msg: " + e.getMessage());
            }

            String analyzed_file_key = "worker-" + outputFileName;

            try {
                AmazonUtils.S3.uploadFile(worker_bucket_name, analyzed_file_key, output_analysis_file);
            } catch (IOException e) {
                System.err.println("Worker Can't upload file: " + outputFileName + ", IOException: " + e.getMessage());
            } catch (S3Exception e) {
                System.err.println("Worker Can't upload file: " + outputFileName + ", AWS S3 error: " + e.awsErrorDetails().errorMessage());
            } catch (SdkClientException e) {
                System.err.println("Worker Can't upload file: " + outputFileName + ", Client-side error: " + e.getMessage());
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
            msg_visible_to_other_workers = true;

            try {
                visibilityExtender.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

        }
    }

    private static Thread getVisibilityExtender(String workers_incoming_queue_url, Message message_in_queue_msg) {
        Thread visibilityExtender = new Thread(() -> {
            //Extend SQS message visibility every 30s for 60s
            try {
                while (!msg_visible_to_other_workers) {

                    try {
                        Thread.sleep(30_000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        System.err.println("Visibility extender interrupted");
                        break;
                    }

                    AmazonUtils.SQS.sqs.changeMessageVisibility(
                            ChangeMessageVisibilityRequest.builder()
                                    .queueUrl(workers_incoming_queue_url)
                                    .receiptHandle(message_in_queue_msg.receiptHandle())
                                    .visibilityTimeout(60)
                                    .build());
                }

                // Delete message from the incoming queue
                if(!message_in_queue_msg.body().equals(Config.msg_terminate_string))
                    AmazonUtils.SQS.DeleteMessage(workers_incoming_queue_url, message_in_queue_msg);

            } catch (Exception e) {
                System.err.println("Error in visibility extender: " + e.getMessage());
            }
        });
        visibilityExtender.start();
        return visibilityExtender;
    }
}
