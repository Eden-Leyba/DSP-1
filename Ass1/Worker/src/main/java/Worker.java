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
        String workers_output_queue_url = AmazonUtils.SQS.getQueueURL(Config.workers_done_queue_name);
        String workers_input_queue_url  = AmazonUtils.SQS.getQueueURL(Config.workers_incoming_queue_name);

        Message first_message_in_queue_msg =
                AmazonUtils.SQS.receiveFirstMessage(workers_input_queue_url);
        String body = first_message_in_queue_msg.body();

        long local_id = Long.parseLong(body.split("\n")[0]);
        String requested_analysis = body.split("\n")[1];   // "POS" / "CONSTITUENCY" / "DEPENDENCY"
        String input_file_to_analyze_url = body.split("\n")[2];

        //  Download input file
        File input_File_to_analyze = downloadUsingWget(input_file_to_analyze_url);
        String textBuffer = Files.readString(input_File_to_analyze.toPath());

        // Prepare output file
        String output_File_Name = "summary_" + input_File_to_analyze.getName();
        File output_summery_File = new File(output_File_Name);

        //  Run parser
        StanfordParser parser = new StanfordParser();
        StanfordParser.AnalysisType analysisType =
                StanfordParser.AnalysisType.valueOf(requested_analysis);

        try (PrintWriter writer = new PrintWriter(new FileWriter(output_summery_File))) {
            parser.parseTextBuffer(textBuffer, analysisType, writer);
        }

        //  Upload result to S3
        long time_in_mill = System.currentTimeMillis();
        String worker_bucket_name = "dsp1-task2-" + time_in_mill;
        AmazonUtils.S3.createBucket(worker_bucket_name);

        String analyzed_file_key = "worker-" + output_File_Name;

        try {
            AmazonUtils.S3.uploadFile(worker_bucket_name, analyzed_file_key, output_summery_File);
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        } catch (S3Exception e) {
            System.err.println("AWS S3 error: " + e.awsErrorDetails().errorMessage());
        } catch (SdkClientException e) {
            System.err.println("Client-side error: " + e.getMessage());
        }

        //  Send message back to manager
        String message_output_worker =
                local_id + "\n" +
                input_file_to_analyze_url + "\n" +
                worker_bucket_name + "\n" +
                analyzed_file_key + "\n" +
                requested_analysis;

        AmazonUtils.SQS.sendMessage(workers_output_queue_url, message_output_worker);

        // Delete message from the *input* queue
        AmazonUtils.SQS.DeleteMessage(workers_input_queue_url, first_message_in_queue_msg);
    }


    }
}
