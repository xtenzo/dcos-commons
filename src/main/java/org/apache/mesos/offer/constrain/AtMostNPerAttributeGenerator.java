package org.apache.mesos.offer.constrain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.offer.TaskUtils;

/**
 * Ensures that the given Offer’s attributes each have no more than N instances of the task type
 * running on them.
 */
public class AtMostNPerAttributeGenerator implements PlacementRuleGenerator {
    private final int maxTasksPerAttribute;

    public AtMostNPerAttributeGenerator(int maxTasksPerAttribute) {
        this.maxTasksPerAttribute = maxTasksPerAttribute;
    }

    @Override
    public PlacementRule generate(Collection<TaskInfo> tasks) throws StuckDeploymentException {
        // map: attribute => # tasks overlapping with attribute
        Map<String, Integer> attrTaskCounts = new HashMap<>();
        for (TaskInfo task : tasks) {
            for (String attribute : TaskUtils.getOfferAttributeStrings(task)) {
                Integer val = attrTaskCounts.get(attribute);
                if (val == null) {
                    val = 0;
                }
                val++;
                attrTaskCounts.put(attribute, val);
            }
        }
        //note: see the NOT operation below
        List<PlacementRule> avoidRules = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : attrTaskCounts.entrySet()) {
            if (entry.getValue() >= maxTasksPerAttribute) {
                avoidRules.add(new AttributeRule(entry.getKey()));
            }
        }
        return new NotRule(new OrRule(avoidRules));
    }
}
