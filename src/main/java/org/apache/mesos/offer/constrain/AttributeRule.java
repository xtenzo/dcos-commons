package org.apache.mesos.offer.constrain;

import org.apache.mesos.Protos.Offer;

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
        if (offer.getAttributesList().contains(attribute)) {
            return offer;
        } else {
            // attribute not found: return empty offer
            return offer.toBuilder().clearResources().build();
        }
    }
}
