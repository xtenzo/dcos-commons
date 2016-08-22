package org.apache.mesos.scheduler.plan;

import org.apache.mesos.Protos;
import org.apache.mesos.offer.InvalidRequirementException;
import org.apache.mesos.offer.OfferRequirement;
import org.apache.mesos.scheduler.TaskKiller;
import org.apache.mesos.state.StateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by gabriel on 8/20/16.
 */
public class DefaultDeploymentPhase extends DefaultPhase {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPhase.class);

    public static DefaultDeploymentPhase create(
            String name,
            TaskKiller taskKiller,
            List<TaskSpecification> taskSpecs,
            StateStore stateStore) {
        return new DefaultDeploymentPhase(name, getBlocks(taskSpecs, taskKiller, stateStore));
    }

    /**
     * Constructs a new {@link DefaultPhase}. Intentionally visible to subclasses.
     *
     * @param name   The name of the Phase.
     * @param blocks The blocks contained in the Phase.
     */
    public DefaultDeploymentPhase(String name, Collection<? extends Block> blocks) {
        super(UUID.randomUUID(), name, blocks);
    }

    private static Collection<Block> getBlocks(
            List<TaskSpecification> taskSpecifications,
            TaskKiller taskKiller,
            StateStore stateStore) {

        Collection<Block> blocks = new ArrayList<>();

        for (TaskSpecification taskSpecification : taskSpecifications) {
            Optional<OfferRequirement> optionalOfferRequirement = Optional.empty();
            try {
                optionalOfferRequirement = getOfferRequirement(taskSpecification, stateStore);
                blocks.add(new DefaultBlock(taskSpecification, taskKiller, optionalOfferRequirement));
            } catch (InvalidRequirementException e) {
                LOGGER.error("Failed to create OfferRequirement with exception: ", e);
                return Collections.emptyList();
            }
        }

        return blocks;
    }

    private static Optional<OfferRequirement> getOfferRequirement(
            TaskSpecification taskSpecification,
            StateStore stateStore) throws InvalidRequirementException {
        Optional<Protos.TaskInfo> optionalTaskInfo = stateStore.fetchTask(taskSpecification.getName());

        if (optionalTaskInfo.isPresent()) {
            return getUpdateOfferRequirement(taskSpecification);
        } else {
            return Optional.of(getNewOfferRequirement(taskSpecification));
        }
    }

    private static OfferRequirement getNewOfferRequirement(TaskSpecification taskSpecification)
            throws InvalidRequirementException {
        return new OfferRequirement(Arrays.asList(getNewTaskInfo(taskSpecification)));
    }

    private static Optional<OfferRequirement> getUpdateOfferRequirement(TaskSpecification taskSpecification) {
        return Optional.empty();
    }

    private static Protos.TaskInfo getNewTaskInfo(TaskSpecification taskSpecification) {
        return Protos.TaskInfo.newBuilder()
                .setName(taskSpecification.getName())
                .setTaskId(Protos.TaskID.newBuilder().setValue(""))
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(""))
                .addAllResources(taskSpecification.getResources())
                .setCommand(taskSpecification.getCommand())
                .build();
    }
}
