import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

public class LocalThreadTask implements Callable<Integer> {

    String bucket_name;
    String key;
    long local_id;
    String workers_inoming_tasks_queue_url;
    int n;

    public LocalThreadTask(String bucket_name, String key, long local_id, String workers_inoming_tasks_queue_url, int n) {
        this.bucket_name = bucket_name;
        this.key = key;
        this.local_id = local_id;
        this.workers_inoming_tasks_queue_url = workers_inoming_tasks_queue_url;
        this.n = n;
    }

    public Integer call() {
        //Download the input from S3
        String input_file_content = "";
        try {
            //TODO: what if its not a small file?
            input_file_content = AmazonUtils.S3.getSmallFile(this.bucket_name, this.key);
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
            System.exit(1);
        }
        catch (S3Exception e) {
            System.err.println("AWS S3 error: " + e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        catch (SdkClientException e) {
            System.err.println("Client-side error: " + e.getMessage());
            System.exit(1);
        }

        int num_files_to_process = 0;
        String[] content = input_file_content.split("\n");
        for(String line : content) {
            line = line.replaceAll("\\s+", " ").trim();
            if(line.isEmpty() || line.equals(" ")) {
                continue;
            }

            line = line.replaceAll(" ", "\n");
            String message = local_id + "\n" + line;

            AmazonUtils.SQS.sendMessage(workers_inoming_tasks_queue_url, message);
            System.out.println("Sent message: " + message);
            num_files_to_process++;
        }


        int m = (int) ((float) (num_files_to_process/n) + 0.5);

        //Create an empty S3 file
        String local_app_bucket = "local-"+local_id + "-" + System.currentTimeMillis();
        String html_file_key = "output_file.html";
        try {
            Path emptyFile = Files.createTempFile("output_file", ".html");
            Files.write(emptyFile, new byte[0]);
            AmazonUtils.S3.uploadFile(local_app_bucket, html_file_key, emptyFile.toFile());
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        }

        // Create entry for local in DynamoDB table
        AmazonUtils.DynamoDB.createEntry(
                local_id,
                0,
                num_files_to_process,
                local_app_bucket,
                html_file_key
        );


        /*
        message:
        local_id
        anal_type
        input_url
         */
        return m;
    }
}
