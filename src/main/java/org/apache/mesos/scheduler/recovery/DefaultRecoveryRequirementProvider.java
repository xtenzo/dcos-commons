package org.apache.mesos.scheduler.recovery;

import com.google.protobuf.TextFormat;
import org.apache.mesos.Protos;
import org.apache.mesos.offer.InvalidRequirementException;
import org.apache.mesos.offer.OfferRequirement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by gabriel on 8/21/16.
 */
public class DefaultRecoveryRequirementProvider implements RecoveryRequirementProvider {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public List<RecoveryRequirement> getTransientRecoveryOfferRequirements(List<Protos.TaskInfo> stoppedTasks) {
        List<RecoveryRequirement> transientRecoveryRequirements = new ArrayList<>();

        for (Protos.TaskInfo taskInfo : stoppedTasks) {
            try {
                transientRecoveryRequirements.add(
                        new DefaultRecoveryRequirement(
                                getRestartOfferRequirement(taskInfo),
                                RecoveryRequirement.RecoveryType.TRANSIENT));
            } catch (InvalidRequirementException e) {
                logger.error("Failed to create a RecoveryRequirement for the transiently failed task: " + taskInfo, e);
            }
        }

        return transientRecoveryRequirements;
    }

    @Override
    public List<RecoveryRequirement> getPermanentRecoveryOfferRequirements(List<Protos.TaskInfo> failedTasks) {
        return Collections.emptyList();
    }

    private OfferRequirement getRestartOfferRequirement(Protos.TaskInfo taskInfo) throws InvalidRequirementException {
        Optional<Protos.ExecutorInfo> optionalExecutorInfo = getRestartExecutorInfo(taskInfo);

        final Protos.TaskInfo.Builder replacementTaskInfo = Protos.TaskInfo.newBuilder(taskInfo);
        replacementTaskInfo.clearExecutor();
        replacementTaskInfo.setTaskId(Protos.TaskID.newBuilder().setValue("").build()); // Set later by TaskRequirement


        Protos.TaskInfo restartTaskInfo = replacementTaskInfo.build();

        OfferRequirement offerRequirement = null;
        if (optionalExecutorInfo.isPresent()) {
            offerRequirement = new OfferRequirement(
                    Arrays.asList(restartTaskInfo),
                    optionalExecutorInfo.get(),
                    null,
                    null);
        } else {
            offerRequirement = new OfferRequirement(Arrays.asList(restartTaskInfo));
        }

        String executorInfoString = optionalExecutorInfo.isPresent() ?
                TextFormat.shortDebugString(optionalExecutorInfo.get()) : "";

        logger.info(String.format("Got replacement OfferRequirement: TaskInfo: '%s' ExecutorInfo: '%s'",
                TextFormat.shortDebugString(restartTaskInfo), executorInfoString));

        return offerRequirement;
    }

    private Optional<Protos.ExecutorInfo> getRestartExecutorInfo(Protos.TaskInfo taskInfo) {
        if (!taskInfo.hasExecutor()) {
            return Optional.empty();
        }

        return Optional.of(
                Protos.ExecutorInfo.newBuilder(taskInfo.getExecutor())
                        .setExecutorId(Protos.ExecutorID.newBuilder().setValue(""))
                        .build());
    }
}
