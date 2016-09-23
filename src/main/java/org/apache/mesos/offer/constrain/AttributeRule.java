package org.apache.mesos.offer.constrain;

import org.apache.mesos.Protos.Attribute;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.offer.AttributeStringUtils;

/**
 * Requires that the Offer contain a provided attribute.
 */
public class AttributeRule implements PlacementRule {

    private final String attribute;

    public AttributeRule(String attribute) {
        this.attribute = attribute;
    }

    @Override
    public Offer filter(Offer offer) {
        for (Attribute attributeProto : offer.getAttributesList()) {
            if (attribute.equals(AttributeStringUtils.toString(attributeProto))) {
                // offer attribute found. return entire offer as-is
                return offer;
            }
        }
        // attribute not found: return empty offer
        return offer.toBuilder().clearResources().build();
    }

    @Override
    public String toString() {
        return String.format("AttributeRule{attribute=%s}", attribute);
    }

    /**
     * A generator which returns an {@link AttributeRule} for the provided attribute.
     */
    public static class Generator extends PassthroughGenerator {
        public Generator(String attribute) {
            super(new AttributeRule(attribute));
        }
    }
}
