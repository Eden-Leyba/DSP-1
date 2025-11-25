import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.util.concurrent.Callable;

public class LocalThreadTask implements Callable<Integer> {

    String bucket_name;
    String key;
    int local_id;
    String workers_input_queue_url;
    int n;

    public LocalThreadTask(String bucket_name, String key, int local_id, String workers_input_queue_url, int n) {
        this.bucket_name = bucket_name;
        this.key = key;
        this.local_id = local_id;
        this.workers_input_queue_url = workers_input_queue_url;
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

            AmazonUtils.SQS.sendMessage(workers_input_queue_url, message);
            System.out.println("Sent message: " + message);
            num_files_to_process++;
        }


        int m = (int) ((float) (num_files_to_process/n) + 0.5);


        /*
        message:
        local_id
        anal_type
        input_url
         */
        return m;
    }
}
