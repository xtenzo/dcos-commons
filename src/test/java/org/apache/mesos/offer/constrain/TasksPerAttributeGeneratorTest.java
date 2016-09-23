package org.apache.mesos.offer.constrain;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;

import org.apache.mesos.Protos.Attribute;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.Value;
import org.apache.mesos.offer.TaskUtils;
import org.apache.mesos.testutils.OfferTestUtils;
import org.apache.mesos.testutils.TaskTestUtils;

/**
 * Tests for {@link TasksPerAttributeGenerator}.
 */
public class TasksPerAttributeGeneratorTest {

    private static final Offer OFFER_NO_ATTRS = getOfferWithResources();
    private static final Offer OFFER_TEXT_ATTR;
    private static final Offer OFFER_SET_ATTR;
    static {
        Attribute.Builder a = Attribute.newBuilder()
                .setType(Value.Type.TEXT)
                .setName("footext");
        a.getTextBuilder().setValue("bar");
        OFFER_TEXT_ATTR = OFFER_NO_ATTRS.toBuilder().addAttributes(a.build()).build();
        a = Attribute.newBuilder()
                .setType(Value.Type.SET)
                .setName("fooset");
        a.getSetBuilder().addItem("bar").addItem("baz");
        OFFER_SET_ATTR = OFFER_NO_ATTRS.toBuilder().addAttributes(a.build()).build();
    }
    private static final String TEXT_ATTR = "footext:bar";
    private static final String SET_ATTR = "fooset:{bar,baz}";

    private static final TaskInfo TASK_NO_ATTRS = TaskTestUtils.getTaskInfo(Collections.emptyList());
    private static final TaskInfo TASK_TEXT_ATTR;
    private static final TaskInfo TASK_SET_ATTR;
    static {
        TASK_TEXT_ATTR = TaskUtils.setOfferAttributes(TASK_NO_ATTRS.toBuilder(), OFFER_TEXT_ATTR).build();
        TASK_SET_ATTR = TaskUtils.setOfferAttributes(TASK_NO_ATTRS.toBuilder(), OFFER_SET_ATTR).build();
    }

    @Test
    public void testZeroLimit() throws StuckDeploymentException {
        PlacementRule rule = new TasksPerAttributeGenerator(0)
                .generate(Arrays.asList(TASK_NO_ATTRS, TASK_TEXT_ATTR, TASK_SET_ATTR));
        assertEquals(rule.toString(), 0, rule.filter(OFFER_NO_ATTRS).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_TEXT_ATTR).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_SET_ATTR).getResourcesCount());

        rule = new TasksPerAttributeGenerator(0).generate(Arrays.asList());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_NO_ATTRS).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_TEXT_ATTR).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_SET_ATTR).getResourcesCount());

        rule = new TasksPerAttributeGenerator(0, TEXT_ATTR)
                .generate(Arrays.asList(TASK_NO_ATTRS, TASK_TEXT_ATTR, TASK_SET_ATTR));
        assertEquals(rule.toString(), 0, rule.filter(OFFER_NO_ATTRS).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_TEXT_ATTR).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_SET_ATTR).getResourcesCount());

        rule = new TasksPerAttributeGenerator(0, TEXT_ATTR)
                .generate(Arrays.asList());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_NO_ATTRS).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_TEXT_ATTR).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_SET_ATTR).getResourcesCount());

        rule = new TasksPerAttributeGenerator(0, TEXT_ATTR, SET_ATTR)
                .generate(Arrays.asList(TASK_NO_ATTRS, TASK_TEXT_ATTR, TASK_SET_ATTR));
        assertEquals(rule.toString(), 0, rule.filter(OFFER_NO_ATTRS).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_TEXT_ATTR).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_SET_ATTR).getResourcesCount());

        rule = new TasksPerAttributeGenerator(0, TEXT_ATTR, SET_ATTR)
                .generate(Arrays.asList());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_NO_ATTRS).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_TEXT_ATTR).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_SET_ATTR).getResourcesCount());
    }

    @Test
    public void testOneLimit() throws StuckDeploymentException {
        PlacementRule rule = new TasksPerAttributeGenerator(1)
                .generate(Arrays.asList(TASK_NO_ATTRS, TASK_TEXT_ATTR, TASK_SET_ATTR));
        assertEquals(rule.toString(), 0, rule.filter(OFFER_NO_ATTRS).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_TEXT_ATTR).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_SET_ATTR).getResourcesCount());

        rule = new TasksPerAttributeGenerator(1).generate(Arrays.asList());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_NO_ATTRS).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_TEXT_ATTR).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_SET_ATTR).getResourcesCount());

        rule = new TasksPerAttributeGenerator(1, TEXT_ATTR)
                .generate(Arrays.asList(TASK_NO_ATTRS, TASK_TEXT_ATTR, TASK_SET_ATTR));
        assertEquals(rule.toString(), 0, rule.filter(OFFER_NO_ATTRS).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_TEXT_ATTR).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_SET_ATTR).getResourcesCount());

        rule = new TasksPerAttributeGenerator(1, TEXT_ATTR)
                .generate(Arrays.asList());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_NO_ATTRS).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_TEXT_ATTR).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_SET_ATTR).getResourcesCount());

        rule = new TasksPerAttributeGenerator(1, TEXT_ATTR, SET_ATTR)
                .generate(Arrays.asList(TASK_NO_ATTRS, TASK_TEXT_ATTR, TASK_SET_ATTR));
        assertEquals(rule.toString(), 0, rule.filter(OFFER_NO_ATTRS).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_TEXT_ATTR).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_SET_ATTR).getResourcesCount());

        rule = new TasksPerAttributeGenerator(1, TEXT_ATTR, SET_ATTR)
                .generate(Arrays.asList());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_NO_ATTRS).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_TEXT_ATTR).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_SET_ATTR).getResourcesCount());
    }

    @Test
    public void testTwoLimit() throws StuckDeploymentException {
        PlacementRule rule = new TasksPerAttributeGenerator(2)
                .generate(Arrays.asList(TASK_NO_ATTRS, TASK_TEXT_ATTR, TASK_SET_ATTR));
        assertEquals(rule.toString(), 0, rule.filter(OFFER_NO_ATTRS).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_TEXT_ATTR).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_SET_ATTR).getResourcesCount());

        rule = new TasksPerAttributeGenerator(2).generate(Arrays.asList());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_NO_ATTRS).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_TEXT_ATTR).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_SET_ATTR).getResourcesCount());

        rule = new TasksPerAttributeGenerator(2, TEXT_ATTR)
                .generate(Arrays.asList(TASK_NO_ATTRS, TASK_TEXT_ATTR, TASK_SET_ATTR));
        assertEquals(rule.toString(), 0, rule.filter(OFFER_NO_ATTRS).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_TEXT_ATTR).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_SET_ATTR).getResourcesCount());

        rule = new TasksPerAttributeGenerator(2, TEXT_ATTR)
                .generate(Arrays.asList());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_NO_ATTRS).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_TEXT_ATTR).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_SET_ATTR).getResourcesCount());

        rule = new TasksPerAttributeGenerator(2, TEXT_ATTR, SET_ATTR)
                .generate(Arrays.asList(TASK_NO_ATTRS, TASK_TEXT_ATTR, TASK_SET_ATTR));
        assertEquals(rule.toString(), 0, rule.filter(OFFER_NO_ATTRS).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_TEXT_ATTR).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_SET_ATTR).getResourcesCount());

        rule = new TasksPerAttributeGenerator(2, TEXT_ATTR, SET_ATTR)
                .generate(Arrays.asList());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_NO_ATTRS).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_TEXT_ATTR).getResourcesCount());
        assertEquals(rule.toString(), 0, rule.filter(OFFER_SET_ATTR).getResourcesCount());
    }

    //TODO(nick): test stuck deployment, filtering of specific attribute

    private static Offer getOfferWithResources() {
        Offer.Builder o = OfferTestUtils.getEmptyOfferBuilder();
        OfferTestUtils.addResource(o, "a");
        OfferTestUtils.addResource(o, "b");
        return o.build();
    }
}
