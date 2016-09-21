package org.apache.mesos.executor;

import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.TaskUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.*;

/**
 * A ThreadTask is a task run by an Executor as a Thread.
 */
public class ScheduledThreadTask implements ScheduledExecutorTask {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ScheduledExecutorService executorService;
    private final Runnable runnable;
    private final boolean exitOnTermination;
    private final ExecutorDriver driver;
    private final Protos.TaskInfo taskInfo;
    private final Duration interval;
    private ScheduledFuture<?> future;

    public ScheduledThreadTask(
            ExecutorDriver driver,
            Protos.TaskInfo taskInfo,
            ScheduledExecutorService executorService,
            Runnable runnable,
            Duration interval,
            boolean exitOnTermination) {
        this.driver = driver;
        this.taskInfo = taskInfo;
        this.executorService = executorService;
        this.runnable = runnable;
        this.interval = interval;
        this.exitOnTermination = exitOnTermination;
    }

    @Override
    public void stop() {
        future.cancel(true);
    }

    @Override
    public void run() {
        future = executorService.scheduleAtFixedRate(runnable, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
        boolean succeeded = false;

        while(true) {
            try {
                future.get();
                succeeded = true;
                sendStatus(Protos.TaskState.TASK_FINISHED);
                break;
            } catch (CancellationException e) {
                logger.warn("ThreadTask: " + runnable + " was cancelled with exception: ", e);
                sendStatus(Protos.TaskState.TASK_KILLED, runnable.toString() + " was cancelled.");
                break;
            } catch (InterruptedException e) {
                logger.warn("Interrupted while waiting for ThreadTask: " + runnable + " with exception: ", e);
                // N.B. we do not break the infinite loop here as this thread has been interrupted, but has not
                // completed waiting for the result of the Callable.
            } catch (ExecutionException e) {
                logger.error("ThreadTask: " + runnable + " failed with an exception: ", e);
                sendStatus(Protos.TaskState.TASK_FAILED, runnable.toString() + " failed with exception " + e);
                break;
            }
        }

        if (exitOnTermination) {
            System.exit(succeeded ?
                    ExecutorErrorCode.EXIT_ON_TERMINATION_SUCCESS.ordinal() :
                    ExecutorErrorCode.EXIT_ON_TERMINATION_FAILURE.ordinal());
        }
    }

    private void sendStatus(Protos.TaskState taskState, String msg) {
        TaskUtils.sendStatus(
                driver,
                taskState,
                taskInfo.getTaskId(),
                taskInfo.getSlaveId(),
                taskInfo.getExecutor().getExecutorId(),
                msg);
    }

    @Override
    public Duration getDelay() {
        return Duration.ZERO;
    }

    @Override
    public Duration getInterval() {
        return interval;
    }
}
