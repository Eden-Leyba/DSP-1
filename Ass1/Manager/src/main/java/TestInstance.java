import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.io.IOException;

public class TestInstance {
    public static void main2(String[] args) {
        String bucket_name = "dsp1-task1-" + System.currentTimeMillis();
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

        String locals_output_queue_url =  AmazonUtils.SQS.getQueueURL(Config.locals_output_queue_name);
        String message_body = "dsp-1111111" + "\n" + "noa";
        AmazonUtils.SQS.sendMessage(locals_output_queue_url ,message_body);
        System.out.println("Sent message: " + message_body.replaceAll("\n", ";"));

        File inputFile = new File("test.txt");
        String key = "local-app-" + "test.txt";
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
    }
}
