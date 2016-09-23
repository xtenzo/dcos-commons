package org.apache.mesos.offer.constrain;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.offer.TaskUtils;

/**
 * Requires that the Offer be from the provided agent ID.
 */
public class AgentRule implements PlacementRule {

    private final String agentId;

    public AgentRule(String agentId) {
        this.agentId = agentId;
    }

    @Override
    public Offer filter(Offer offer) {
        if (offer.getSlaveId().getValue().equals(agentId)) {
            return offer;
        } else {
            // agent mismatch: return empty offer
            return offer.toBuilder().clearResources().build();
        }
    }

    @Override
    public String toString() {
        return String.format("AgentRule{agentId=%s}", agentId);
    }

    /**
     * Ensures that the given Offer is colocated with the specified agent ID.
     */
    public static class ColocateAgentGenerator extends PassthroughGenerator {

        public ColocateAgentGenerator(String agentId) {
            super(new AgentRule(agentId));
        }
    }

    /**
     * Ensures that the given Offer is NOT colocated with the specified agent ID.
     */
    public static class AvoidAgentGenerator extends PassthroughGenerator {

        public AvoidAgentGenerator(String agentId) {
            super(new NotRule(new AgentRule(agentId)));
        }
    }

    /**
     * Ensures that the given Offer is colocated with one of the specified agent IDs.
     */
    public static class ColocateAgentsGenerator extends PassthroughGenerator {

        public ColocateAgentsGenerator(Collection<String> agentIds) {
            super(new OrRule(toAgentRules(agentIds)));
        }

        public ColocateAgentsGenerator(String... agentIds) {
            this(Arrays.asList(agentIds));
        }
    }

    /**
     * Ensures that the given Offer is NOT colocated with any of the specified agent IDs.
     */
    public static class AvoidAgentsGenerator extends PassthroughGenerator {

        public AvoidAgentsGenerator(Collection<String> agentIds) {
            super(new NotRule(new OrRule(toAgentRules(agentIds))));
        }

        public AvoidAgentsGenerator(String... agentIds) {
            this(Arrays.asList(agentIds));
        }
    }

    /**
     * Ensures that the given Offer is colocated with the specified node type.
     */
    public static class ColocateTypeGenerator implements PlacementRuleGenerator {

        private final String typeToColocateWith;

        public ColocateTypeGenerator(String typeToColocateWith) {
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

    /**
     * Ensures that the given Offer is NOT colocated with the specified node type.
     */
    public static class AvoidTypeGenerator implements PlacementRuleGenerator {

        private final PlacementRuleGenerator generator;

        public AvoidTypeGenerator(String typeToAvoid) {
            generator = new NotRule.Generator(new ColocateTypeGenerator(typeToAvoid));
        }

        @Override
        public PlacementRule generate(Collection<TaskInfo> tasks) throws StuckDeploymentException {
            return generator.generate(tasks);
        }
    }

    /**
     * Converts the provided agent ids into {@link AgentRule}s.
     */
    private static Collection<PlacementRule> toAgentRules(Collection<String> agentIds) {
        List<PlacementRule> rules = new ArrayList<>();
        for (String agentId : agentIds) {
            rules.add(new AgentRule(agentId));
        }
        return rules;
    }
}
