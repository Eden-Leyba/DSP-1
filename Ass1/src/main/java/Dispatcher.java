
class Dispatcher {
    public static void main(String[] args) throws Exception {
        if (args.length == 1){
            if(args[0].equals("Manager"))
            {
                Manager.ManagerMain();
            }
            if(args[0].equals("Worker"))
            {
                Worker.WorkerMain();
            }
        }
        else {
            LocalApp.LocalMain(args);
        }
    }
}
