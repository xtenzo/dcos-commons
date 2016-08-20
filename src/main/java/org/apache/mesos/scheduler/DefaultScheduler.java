package org.apache.mesos.scheduler;

import com.google.protobuf.TextFormat;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.curator.CuratorStateStore;
import org.apache.mesos.reconciliation.DefaultReconciler;
import org.apache.mesos.reconciliation.Reconciler;
import org.apache.mesos.state.StateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by gabriel on 8/20/16.
 */
public class DefaultScheduler implements Scheduler {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final StateStore stateStore;
    private final Reconciler reconciler;

    private boolean isRegistered = false;

    public DefaultScheduler(String frameworkName) {
        this.stateStore = new CuratorStateStore(frameworkName);
        this.reconciler = new DefaultReconciler(stateStore);
    }

    @Override
    public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
        logger.info("Registered framework with frameworkId: " + frameworkId.getValue());
        try {
            stateStore.storeFrameworkId(frameworkId);
            isRegistered = true;
        } catch (Exception e) {
            isRegistered = false;
            logger.error(String.format(
                    "Unable to store registered framework ID '%s'", frameworkId.getValue()), e);
        }
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
        logger.error("Registered framework with master: " + masterInfo);
        hardExit(SchedulerErrorCode.REREGISTERED);
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        logOffers(offers);
        reconciler.reconcile(driver);

    }

    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {

    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {

    }

    @Override
    public void frameworkMessage(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {

    }

    @Override
    public void disconnected(SchedulerDriver driver) {

    }

    @Override
    public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {

    }

    @Override
    public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, int status) {

    }

    @Override
    public void error(SchedulerDriver driver, String message) {

    }

    private void logOffers(List<Protos.Offer> offers) {
        if (offers == null) {
            return;
        }

        logger.info(String.format("Received %d offers:", offers.size()));
        for (int i = 0; i < offers.size(); ++i) {
            // offer protos are very long. print each as a single line:
            logger.info(String.format("- Offer %d: %s", i + 1, TextFormat.shortDebugString(offers.get(i))));
        }
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings("DM_EXIT")
    private void hardExit(SchedulerErrorCode errorCode) {
        System.exit(errorCode.ordinal());
    }
}
