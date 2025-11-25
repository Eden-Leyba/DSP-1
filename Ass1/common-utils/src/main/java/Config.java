import software.amazon.awssdk.regions.Region;

public class Config {

    final static String locals_output_queue_name = "LocalsOutput";
    final static String locals_input_queue_name = "LocalsInput";
    final static String workers_output_queue_name = "WorkersOutput";
    final static String workers_input_queue_name = "WorkersInput";

    final static Region region = Region.US_EAST_1;
    final static String manager_role_value = "manager";
    final static String worker_role_value = "worker";
    final static String instances_tag_name = "Role";

    final static String aws_folder_path = "C:\\Users\\edenl\\.aws";

    final static int MAX_WORKER_INSTANCES = 18;
}
