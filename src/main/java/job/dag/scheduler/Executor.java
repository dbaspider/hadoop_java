package job.dag.scheduler;

public interface Executor {
    boolean execute(TaskCallBack callBack);
}