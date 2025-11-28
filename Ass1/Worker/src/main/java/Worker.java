import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.nio.file.Files;


public class Worker {
    private static File downloadUsingWget(String url) throws IOException, InterruptedException {

        File output = File.createTempFile("input-", ".txt");
        String outputPath = output.getAbsolutePath();

        // Build the wget command
        ProcessBuilder pb = new ProcessBuilder(
                "wget", url, "-O", outputPath
        );

        pb.redirectErrorStream(true); // merge stderr and stdout
        Process process = pb.start();

        // Read wget output (optional)
        InputStream is = process.getInputStream();
        String text = new String(is.readAllBytes());
        System.out.println("WGET OUTPUT:\n" + text);

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IOException("wget failed with exit code " + exitCode);
        }

        return output;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String workers_done_queue_url = AmazonUtils.SQS.getQueueURL(Config.workers_done_queue_name);
        String workers_incoming_queue_url = AmazonUtils.SQS.getQueueURL(Config.workers_incoming_queue_name);

        Message first_message_in_queue_msg =
                AmazonUtils.SQS.receiveFirstMessage(workers_incoming_queue_url);
        String body = first_message_in_queue_msg.body();

        String[] msg_spilt = body.split("\n");
        long local_id = Long.parseLong(msg_spilt[0]);
        String requested_analysis = msg_spilt[1];   // "POS" / "CONSTITUENCY" / "DEPENDENCY"
        String input_file_to_analyze_url = msg_spilt[2];

        // Download input file
        File input_File_to_analyze = downloadUsingWget(input_file_to_analyze_url);
        String textBuffer = Files.readString(input_File_to_analyze.toPath());

        // Prepare output file
        String output_File_Name = "analysis_" + input_File_to_analyze.getName();
        File output_analysis_File = new File(output_File_Name);

        // Run parser
        StanfordParser parser = new StanfordParser();
        StanfordParser.AnalysisType analysisType =
                StanfordParser.AnalysisType.valueOf(requested_analysis);

        try (PrintWriter writer = new PrintWriter(new FileWriter(output_analysis_File))) {
            parser.parseTextBuffer(textBuffer, analysisType, writer);
        } catch (IOException e) {
            System.err.println("Parser Error: " + e.getMessage());
        }

        // Upload result to S3
        long time_in_mill = System.currentTimeMillis();
        String worker_bucket_name = "dsp1-task1-" + time_in_mill;
        try {
            AmazonUtils.S3.createBucket(worker_bucket_name);
        } catch (S3Exception | SdkClientException e) {
            System.err.println("Worker Can't create bucket with name: " + worker_bucket_name + ", err msg: " + e.getMessage());
        }

        String analyzed_file_key = "worker-" + output_File_Name;

        try {
            AmazonUtils.S3.uploadFile(worker_bucket_name, analyzed_file_key, output_analysis_File);
        } catch (IOException e) {
            System.err.println("Worker Can't create upload file: "+ output_File_Name +", IOException: " + e.getMessage());
        } catch (S3Exception e) {
            System.err.println("Worker Can't create upload file: "+ output_File_Name +", AWS S3 error: " + e.awsErrorDetails().errorMessage());
        } catch (SdkClientException e) {
            System.err.println("Worker Can't create upload file: "+ output_File_Name +", Client-side error: " + e.getMessage());
        }

        //  Send message back to manager
        String message_output_worker =
                local_id + "\n" +
                input_file_to_analyze_url + "\n" +
                worker_bucket_name + "\n" +
                analyzed_file_key + "\n" +
                requested_analysis;

        AmazonUtils.SQS.sendMessage(workers_done_queue_url, message_output_worker);

        // Delete message from the *input* queue
        AmazonUtils.SQS.DeleteMessage(workers_incoming_queue_url, first_message_in_queue_msg);
    }
}
