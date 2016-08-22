package org.apache.mesos.scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by gabriel on 8/20/16.
 */
public class SchedulerUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerUtils.class);

    public static void declineOffers(
            SchedulerDriver driver,
            List<Protos.OfferID> acceptedOffers,
            List<Protos.Offer> offers) {

        for (Protos.Offer offer : offers) {
            Protos.OfferID offerId = offer.getId();
            if (!acceptedOffers.contains(offerId)) {
                LOGGER.info("Declining offer: " + offerId.getValue());
                driver.declineOffer(offerId);
            }
        }
    }

    public static String nameToRole(String frameworkName) {
        return frameworkName + "-role";
    }

    public static String nameToPrincipal(String frameworkName) {
        return frameworkName + "-principal";
    }

    public static List<Protos.Offer> filterAcceptedOffers(
            List<Protos.Offer> offers,
            List<Protos.OfferID> acceptedOfferIds) {

        List<Protos.Offer> filteredOffers = new ArrayList<Protos.Offer>();

        for (Protos.Offer offer : offers) {
            if (!offerAccepted(offer, acceptedOfferIds)) {
                filteredOffers.add(offer);
            }
        }

        return filteredOffers;
    }

    private static boolean offerAccepted(Protos.Offer offer, List<Protos.OfferID> acceptedOfferIds) {
        for (Protos.OfferID acceptedOfferId: acceptedOfferIds) {
            if (acceptedOfferId.equals(offer.getId())) {
                return true;
            }
        }

        return false;
    }
}
