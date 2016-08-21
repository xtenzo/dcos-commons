package org.apache.mesos.scheduler.plan;

import org.apache.mesos.scheduler.TaskKiller;
import org.apache.mesos.state.StateStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * Created by gabriel on 8/20/16.
 */
public class DefaultDeploymentPhase extends DefaultPhase {
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
            blocks.add(new DefaultBlock(taskSpecification, taskKiller, stateStore));
        }

        return blocks;
    }
}
