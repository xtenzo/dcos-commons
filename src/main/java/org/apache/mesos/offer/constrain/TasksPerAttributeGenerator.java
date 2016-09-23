package org.apache.mesos.offer.constrain;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.offer.TaskUtils;

/**
 * Ensures that the given Offer’s attributes each have no more than N instances of the task type
 * running on them.
 *
 * For example, this can ensure that no more than 3 tasks are running against the 'rack:foo'
 * attribute.
 */
public class TasksPerAttributeGenerator implements PlacementRuleGenerator {

    private final int maxTasksPerAttribute;
    private final Set<String> attributesToCheck;

    /**
     * Creates a new rule generator which will block deployment on tasks which already have N
     * instances running against a provided list of attributes, or against any/all attributes if the
     * provided list is empty.
     */
    public TasksPerAttributeGenerator(
            int maxTasksPerAttribute, Collection<String> attributesToCheck) {
        this.maxTasksPerAttribute = maxTasksPerAttribute;
        this.attributesToCheck = new HashSet<>(attributesToCheck);
    }

    public TasksPerAttributeGenerator(int maxTasksPerAttribute, String... attributesToCheck) {
        this(maxTasksPerAttribute, Arrays.asList(attributesToCheck));
    }

    @Override
    public PlacementRule generate(Collection<TaskInfo> tasks) throws StuckDeploymentException {
        // map: enforced attribute => # tasks which were launched against attribute
        Map<String, Integer> attrTaskCounts = new HashMap<>();
        for (TaskInfo task : tasks) {
            for (String attribute : TaskUtils.getOfferAttributeStrings(task)) {
                // only enforce listed attributes, when the list is non-empty:
                if (!attributesToCheck.isEmpty() && !attributesToCheck.contains(attribute)) {
                    continue;
                }
                // increment the count for this attribute:
                Integer val = attrTaskCounts.get(attribute);
                if (val == null) {
                    val = 0;
                }
                val++;
                attrTaskCounts.put(attribute, val);
            }
        }
        // find the attributes which meet or exceed the limit and block any offers with those
        // attributes from the next launch.
        List<PlacementRule> blockedAttributes = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : attrTaskCounts.entrySet()) {
            if (entry.getValue() >= maxTasksPerAttribute) {
                blockedAttributes.add(new AttributeRule(entry.getKey()));
            }
        }
        if (blockedAttributes.isEmpty()) {
            // nothing is full, don't filter any offers.
            return PassthroughRule.getInstance();
        } else {
            // filter out any offers which contain the full attributes.
            return new NotRule(new OrRule(blockedAttributes));
        }
    }
}
