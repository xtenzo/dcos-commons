package org.apache.mesos.scheduler.plan;

import org.apache.mesos.Protos;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.TaskKiller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Created by gabriel on 8/20/16.
 */
public class DefaultBlock implements Block {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final UUID blockId = UUID.randomUUID();
    private final TaskSpecification taskSpecification;
    private final TaskKiller taskKiller;
    private final OfferRequirement offerRequirement;
    private Status status = Status.PENDING;

    public DefaultBlock(TaskSpecification taskSpecification, TaskKiller taskKiller, OfferRequirement offerRequirement) {
        this.taskSpecification = taskSpecification;
        this.taskKiller = taskKiller;
        this.offerRequirement = offerRequirement;
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
        return null;
    }

    @Override
    public void updateOfferStatus(boolean accepted) {
        if (accepted) {
            setStatus(Status.IN_PROGRESS);
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
}
