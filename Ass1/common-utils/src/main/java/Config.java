import software.amazon.awssdk.regions.Region;

public class Config {

    final static String locals_output_queue_name = "LocalsOutput";
    final static String locals_input_queue_name = "LocalsInput";
    final static String workers_done_queue_name = "Workers_Tasks_Done";
    final static String workers_incoming_queue_name = "Workers_Tasks_Incoming";

    final static Region region = Region.US_EAST_1;
    final static String manager_role_value = "manager";
    final static String worker_role_value = "worker";
    final static String instances_tag_name = "Role";

    final static String aws_folder_path = "C:\\Users\\user\\.aws";

    final static int MAX_WORKER_INSTANCES = 18;

    final static String JAR_BUCKET = "jars-1763844625474";
}
