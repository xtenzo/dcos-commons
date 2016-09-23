package org.apache.mesos.executor;

import java.util.concurrent.Future;

/**
 * A LaunchedTask encapsulates a Task launched on an Executor (ExecutorTask) and the Future representing its execution.
 */
public class LaunchedTask {
    private final ExecutorTask executorTask;
    private final Future<?> future;

    public LaunchedTask(ExecutorTask executorTask, Future<?> future) {
        this.executorTask = executorTask;
        this.future = future;
    }

    public ExecutorTask getExecutorTask() {
        return executorTask;
    }

    public Future<?> getFuture() {
        return future;
    }
}
