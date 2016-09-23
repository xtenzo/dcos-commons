package org.apache.mesos.offer.constrain;

import java.util.Collection;

import org.apache.mesos.Protos.TaskInfo;

/**
 * Dynamically creates {@link PlacementRule}s which depend on the current deployed state of the
 * system.
 */
public interface PlacementRuleGenerator {

    /**
     * Returns a new {@link PlacementRule} which defines where a task may be placed given the
     * current deployed state of the system.
     *
     * @throws StuckDeploymentException if the resulting PlacementRule would disallow deployment at
     *     any location, effectively putting deployment in a stuck state
     */
    public PlacementRule generate(Collection<TaskInfo> tasks) throws StuckDeploymentException;
}
