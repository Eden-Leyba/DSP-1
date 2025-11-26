import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;


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
        String workers_output_queue_url =  AmazonUtils.SQS.getQueueURL(Config.workers_done_queue_name);
        String workers_input_queue_url =  AmazonUtils.SQS.getQueueURL(Config.workers_incoming_queue_name);

        String first_message_in_queue =  AmazonUtils.SQS.receiveFirstMessage(workers_input_queue_url);

        int local_id = Integer.parseInt(first_message_in_queue.split("\n")[0]);
        String requested_analysis = first_message_in_queue.split("\n")[1];
        String input_file_to_analyze_url = first_message_in_queue.split("\n")[2];

        File inputFile = downloadUsingWget(input_file_to_analyze_url);

        long time_in_mill = System.currentTimeMillis();
        String worker_bucket_name = "dsp1-task2-" + time_in_mill;
        //todo: create bucket
        String inputFileName = first_message_in_queue.split("\n")[0] + "-" + time_in_mill;
        String analyzed_file_key = "worker-" + inputFileName;
        try {
            AmazonUtils.S3.uploadFile(worker_bucket_name, analyzed_file_key, inputFile);
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        }
        catch (S3Exception e) {
            System.err.println("AWS S3 error: " + e.awsErrorDetails().errorMessage());
        }
        catch (SdkClientException e) {
            System.err.println("Client-side error: " + e.getMessage());
        }


        String message = local_id + "\n" + input_file_to_analyze_url + "\n" + worker_bucket_name + "\n" + analyzed_file_key + "\n" + requested_analysis;
        AmazonUtils.SQS.sendMessage(workers_output_queue_url, message);
        AmazonUtils.SQS.DeleteMessage(workers_intput_queue_url)



    }
}
