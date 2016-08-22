package org.apache.mesos.scheduler.plan;

import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.TaskKiller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Created by gabriel on 8/20/16.
 */
public class DefaultBlock implements Block {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final UUID blockId = UUID.randomUUID();
    private final TaskSpecification taskSpecification;
    private final TaskKiller taskKiller;
    private final Optional<OfferRequirement> optionalOfferRequirement;
    private Status status;
    private Set<Protos.TaskID> taskIds = Collections.emptySet();

    public DefaultBlock(
            TaskSpecification taskSpecification,
            TaskKiller taskKiller,
            Optional<OfferRequirement> optionalOfferRequirement,
            Status initialStatus) {
        this.taskSpecification = taskSpecification;
        this.taskKiller = taskKiller;
        this.optionalOfferRequirement = optionalOfferRequirement;
        this.status = initialStatus;
    }

    public DefaultBlock(
            TaskSpecification taskSpecification,
            TaskKiller taskKiller,
            Optional<OfferRequirement> optionalOfferRequirement) {
        this(taskSpecification, taskKiller, optionalOfferRequirement, Status.PENDING);
    }

    @Override
    public boolean isPending() {
        return status == Status.PENDING;
    }

    @Override
    public boolean isInProgress() {
        return status == Status.IN_PROGRESS;
    }

    @Override
    public boolean isComplete() {
        return status == Status.COMPLETE;
    }

    @Override
    public OfferRequirement start() {
        if (optionalOfferRequirement.isPresent()) {
            return optionalOfferRequirement.get();
        } else {
            return null;
        }
    }

    @Override
    public void updateOfferStatus(Optional<Set<Protos.TaskID>> optionalTaskIds) {
        if (optionalTaskIds.isPresent()) {
            setStatus(Status.IN_PROGRESS);
            setTaskIds(optionalTaskIds.get());
        } else {
            setStatus(Status.PENDING);
        }
    }

    @Override
    public void restart() {
        setStatus(Status.PENDING);
    }

    @Override
    public void forceComplete() {
    }

    @Override
    public void update(Protos.TaskStatus status) {
        if (status.getReason().equals(Protos.TaskStatus.Reason.REASON_RECONCILIATION)) {
            logger.warn("Ignoring TaskStatus update due to reconciliation.");
            return;
        }

        logger.info("Updating '" + getName() + "' with TaskStatus: " + status);
        logger.info("TaskIds: " + taskIds);

        if (taskIds.contains(status.getTaskId())) {
            switch (status.getState()) {
                case TASK_RUNNING:
                    setStatus(Status.COMPLETE);
                    break;
                case TASK_ERROR:
                case TASK_FAILED:
                case TASK_FINISHED:
                case TASK_KILLED:
                    setStatus(Status.PENDING);
                    break;
                case TASK_STAGING:
                case TASK_STARTING:
                    setStatus(Status.IN_PROGRESS);
                    break;
                default:
                    logger.warn("Unhandled TaskState: " + status.getState());
            }
        } else {
            logger.warn("Ignoring TaskStatus update from unexpected TaskID: ", status.getTaskId());
        }
    }

    @Override
    public UUID getId() {
        return blockId;
    }

    @Override
    public String getMessage() {
        return getName() + " is " + Block.getStatus(this);
    }

    @Override
    public String getName() {
        return taskSpecification.getName();
    }

    private void setStatus(Status newStatus) {
        Status oldStatus = status;
        status = newStatus;
        logger.info(getName() + ": changed status from: " + oldStatus + " to: " + newStatus);
    }

    private void setTaskIds(Set<Protos.TaskID> taskIds) {
        if (taskIds.isEmpty()) {
            logger.warn("Expected TaskID set is empty.");
        } else {
            logger.info("Setting '" + taskIds.size() + "' expected TaskIDs to: " + taskIds);
        }

        this.taskIds = taskIds;
    }
}
