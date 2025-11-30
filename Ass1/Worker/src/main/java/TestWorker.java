import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.InstanceType;

import java.io.IOException;

public class TestWorker {
    public static void main(String[] args){
        String amiId = "ami-0cae6d6fe6048ca2c";
        try {
            AmazonUtils.EC2.LaunchMultipleInstances(
                    amiId,
                    InstanceType.T3_LARGE,
                    Config.instances_tag_name, Config.worker_role_value,
                    "jars-1763844625474", "Worker.jar",
                    1, 1,
                    false,
                    Config.aws_folder_path
            );
        } catch (Ec2Exception e) {
            if(e.awsErrorDetails().errorCode().equals("InsufficientInstanceCapacity")) {
                System.err.println("Cannot run ec2 instance! Not enough EC2 Capacity!");
            }
        } catch (InterruptedException | IOException ex) {
            throw new RuntimeException(ex);
        }
    }

}