package org.apache.mesos.offer.constrain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.offer.TaskUtils;

/**
 * Ensures that the given Offer is colocated with the specified node type. Wrap this with a
 * {@link NotRule} to get the opposite.
 */
public class ColocateAgentGenerator implements PlacementRuleGenerator {
    private final String typeToColocateWith;

    public ColocateAgentGenerator(String typeToColocateWith) {
        this.typeToColocateWith = typeToColocateWith;
    }

    @Override
    public PlacementRule generate(Collection<TaskInfo> tasks) throws StuckDeploymentException {
        Set<String> colocateAgents = new HashSet<>();
        for (TaskInfo task : tasks) {
            if (TaskUtils.toTaskType(task.getTaskId()).equals(this.typeToColocateWith)) {
                // Matching task type found. Colocate with it on this agent.
                colocateAgents.add(task.getSlaveId().getValue());
            }
        }
        if (colocateAgents.isEmpty()) {
            throw new StuckDeploymentException(
                    "no matching tasks found to colocate, this " +
                    "deployment will never succeed");
        }
        List<PlacementRule> agentRules = new ArrayList<>();
        for (String agent : colocateAgents) {
            agentRules.add(new AgentRule(agent));
        }
        return new OrRule(agentRules);
    }
}
