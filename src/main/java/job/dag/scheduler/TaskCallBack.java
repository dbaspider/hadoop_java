package job.dag.scheduler;

public interface TaskCallBack {
     Object invoke(TaskInstanceResult result);
}