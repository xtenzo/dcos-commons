package org.apache.mesos.offer.constrain;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

import org.apache.mesos.Protos.Attribute;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Value;
import org.apache.mesos.offer.AttributeStringUtils;
import org.apache.mesos.testutils.OfferTestUtils;

/**
 * Tests for {@link AttributeRule}.
 */
public class AttributeRuleTest {

    private static final Attribute ATTR_TEXT;
    private static final Attribute ATTR_SCALAR;
    private static final Attribute ATTR_RANGES;
    private static final Attribute ATTR_SET;
    static {
        Attribute.Builder a = Attribute.newBuilder()
                .setType(Value.Type.TEXT)
                .setName("footext");
        a.getTextBuilder().setValue("bar");
        ATTR_TEXT = a.build();
        a = Attribute.newBuilder()
                .setType(Value.Type.SCALAR)
                .setName("fooscalar");
        a.getScalarBuilder().setValue(123.456);
        ATTR_SCALAR = a.build();
        a = Attribute.newBuilder()
                .setType(Value.Type.RANGES)
                .setName("fooranges");
        a.getRangesBuilder().addRangeBuilder().setBegin(234).setEnd(345);
        a.getRangesBuilder().addRangeBuilder().setBegin(456).setEnd(567);
        ATTR_RANGES = a.build();
        a = Attribute.newBuilder()
                .setType(Value.Type.SET)
                .setName("fooset");
        a.getSetBuilder().addItem("bar").addItem("baz");
        ATTR_SET = a.build();
    }

    @Test
    public void testExactMatches() {
        Offer.Builder o = getOfferWithResources()
                .addAttributes(ATTR_TEXT);
        assertEquals(o.build(),
                new AttributeRule(AttributeStringUtils.toString(ATTR_TEXT)).filter(o.build()));

        o = getOfferWithResources()
                .addAttributes(ATTR_SCALAR);
        assertEquals(o.build(),
                new AttributeRule(AttributeStringUtils.toString(ATTR_SCALAR)).filter(o.build()));

        o = getOfferWithResources()
                .addAttributes(ATTR_RANGES);
        assertEquals(o.build(),
                new AttributeRule(AttributeStringUtils.toString(ATTR_RANGES)).filter(o.build()));

        o = getOfferWithResources()
                .addAttributes(ATTR_SET);
        assertEquals(o.build(),
                new AttributeRule(AttributeStringUtils.toString(ATTR_SET)).filter(o.build()));
    }

    @Test
    public void testAnyMatches() {
        Offer o = getOfferWithResources()
                .addAttributes(ATTR_TEXT)
                .addAttributes(ATTR_SCALAR)
                .addAttributes(ATTR_RANGES)
                .addAttributes(ATTR_SET)
                .build();
        assertEquals(o, new AttributeRule(AttributeStringUtils.toString(ATTR_TEXT)).filter(o));
        assertEquals(o, new AttributeRule(AttributeStringUtils.toString(ATTR_SCALAR)).filter(o));
        assertEquals(o, new AttributeRule(AttributeStringUtils.toString(ATTR_RANGES)).filter(o));
        assertEquals(o, new AttributeRule(AttributeStringUtils.toString(ATTR_SET)).filter(o));
    }

    @Test
    public void testMismatches() {
        Offer o = getOfferWithResources()
                .addAttributes(ATTR_SCALAR)
                .addAttributes(ATTR_SET)
                .build();
        assertEquals(0, new AttributeRule(AttributeStringUtils.toString(ATTR_TEXT)).filter(o).getResourcesCount());
        assertEquals(o, new AttributeRule(AttributeStringUtils.toString(ATTR_SCALAR)).filter(o));
        assertEquals(0, new AttributeRule(AttributeStringUtils.toString(ATTR_RANGES)).filter(o).getResourcesCount());
        assertEquals(o, new AttributeRule(AttributeStringUtils.toString(ATTR_SET)).filter(o));

        o = getOfferWithResources()
                .addAttributes(ATTR_RANGES)
                .addAttributes(ATTR_TEXT)
                .build();
        assertEquals(o, new AttributeRule(AttributeStringUtils.toString(ATTR_TEXT)).filter(o));
        assertEquals(0, new AttributeRule(AttributeStringUtils.toString(ATTR_SCALAR)).filter(o).getResourcesCount());
        assertEquals(o, new AttributeRule(AttributeStringUtils.toString(ATTR_RANGES)).filter(o));
        assertEquals(0, new AttributeRule(AttributeStringUtils.toString(ATTR_SET)).filter(o).getResourcesCount());
    }

    private static Offer.Builder getOfferWithResources() {
        Offer.Builder o = OfferTestUtils.getEmptyOfferBuilder();
        OfferTestUtils.addResource(o, "a");
        OfferTestUtils.addResource(o, "b");
        return o;
    }
}
