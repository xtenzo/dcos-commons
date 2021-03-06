package org.apache.mesos.scheduler.recovery.constrain;

import org.apache.mesos.Protos.Offer.Operation;
import org.apache.mesos.scheduler.recovery.RecoveryRequirement;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link LaunchConstrainer} combinator that ensures that all the given constrainers are satisfied before launching a
 * task. Useful to create policies that need to limit launches to a certain rate, and when it's an off-peak time.
 * <p>
 * N.B. When determining whether a launch can happen, this object will short-circuit if any of its {@link
 * LaunchConstrainer}s reject the task.
 */
public class AllLaunchConstrainer implements LaunchConstrainer {
    private List<LaunchConstrainer> constrainers;

    public AllLaunchConstrainer(LaunchConstrainer... constrainers) {
        this.constrainers = new ArrayList<>();
        for (LaunchConstrainer constrainer : constrainers) {
            this.constrainers.add(constrainer);
        }
    }

    @Override
    public void launchHappened(Operation launchOperation, RecoveryRequirement.RecoveryType recoveryType) {
        for (LaunchConstrainer constrainer : constrainers) {
            constrainer.launchHappened(launchOperation, recoveryType);
        }
    }

    @Override
    public boolean canLaunch(RecoveryRequirement recoveryRequirement) {
        for (LaunchConstrainer constrainer : constrainers) {
            if (!constrainer.canLaunch(recoveryRequirement)) {
                return false;
            }
        }
        return true;
    }
}
