package org.apache.mesos.scheduler;

import com.google.protobuf.TextFormat;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.config.*;
import org.apache.mesos.curator.CuratorConfigStore;
import org.apache.mesos.curator.CuratorStateStore;
import org.apache.mesos.offer.DefaultOperationRecorder;
import org.apache.mesos.offer.OfferAccepter;
import org.apache.mesos.reconciliation.DefaultReconciler;
import org.apache.mesos.reconciliation.Reconciler;
import org.apache.mesos.scheduler.plan.*;
import org.apache.mesos.scheduler.recovery.DefaultFailureListener;
import org.apache.mesos.state.StateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by gabriel on 8/20/16.
 */
public class DefaultScheduler implements Scheduler {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String frameworkName;
    private final ConfigurationValidator configurationValidator;
    private final DefaultServiceSpecification serviceSpecification;

    private StateStore stateStore;
    private ConfigStore configStore;
    private Reconciler reconciler;
    private StageManager stageManager;
    private OfferAccepter offerAccepter;
    private DefaultStageScheduler stageScheduler;
    private TaskKiller taskKiller;

    public DefaultScheduler(String frameworkName,
                            DefaultServiceSpecification serviceSpecification,
                            Collection<ConfigurationValidation> validations) {
        this.frameworkName = frameworkName;
        this.serviceSpecification = serviceSpecification;
        this.configurationValidator = new DefaultConfigurationValidator(validations);
    }

    public DefaultScheduler(String frameworkName, DefaultServiceSpecification serviceSpecification) {
        this(frameworkName, serviceSpecification, Collections.emptyList());
    }

    private void initialize() throws ConfigStoreException {
        this.stateStore = new CuratorStateStore(frameworkName);
        this.configStore = new CuratorConfigStore<EnvironmentConfiguration>(frameworkName);
        this.reconciler = new DefaultReconciler(stateStore);
        this.stageManager = new DefaultStageManager(getStage(), new DefaultStrategyFactory());
        this.offerAccepter = new OfferAccepter(Arrays.asList(new DefaultOperationRecorder(stateStore)));
        this.stageScheduler = new DefaultStageScheduler(offerAccepter);
        this.taskKiller = new DefaultTaskKiller(new DefaultFailureListener(stateStore));
    }

    private Stage getStage() throws ConfigStoreException {
        Collection<ConfigurationValidationError> configurationValidationErrors =
                configurationValidator.validate(
                        getOldConfiguration(),
                        getNewConfiguration());

        List<Phase> phases = Arrays.asList(ReconciliationPhase.create(reconciler));
        phases.addAll(getDeploymentPhases());

        // If config validation had errors, expose them via the Stage.
        Stage stage = configurationValidationErrors.isEmpty()
                ? DefaultStage.fromList(phases)
                : DefaultStage.withErrors(phases, validationErrorsToStrings(configurationValidationErrors));

        return stage;
    }

    private List<Phase> getDeploymentPhases() {
        List<Phase> phases = new ArrayList<>();

        for (Map.Entry<TaskSpecification, Integer> entry : serviceSpecification.getTaskSpecificationMap().entrySet()) {
            TaskSpecification taskSpecification = entry.getKey();
            Integer count = entry.getValue();
            phases.add(
                    DefaultDeploymentPhase.create(
                            "deployment",
                            taskKiller,
                            getTaskSpecifications(taskSpecification, count),
                            stateStore));
        }

        return phases;
    }

    private List<TaskSpecification> getTaskSpecifications(TaskSpecification taskSpecification, Integer count) {
        List<TaskSpecification> taskSpecifications = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            taskSpecifications.add(
                    new DefaultTaskSpecification(
                            taskSpecification.getName() + "-" + i,
                            taskSpecification.getResources(),
                            taskSpecification.getCommand()));
        }

        return taskSpecifications;
    }

    private Optional<Configuration> getOldConfiguration() throws ConfigStoreException {
        return configStore.getTargetConfig(new EnvironmentConfiguration.Factory());
    }

    private Configuration getNewConfiguration() {
        return new EnvironmentConfiguration();
    }

    private List<String> validationErrorsToStrings(Collection<ConfigurationValidationError> validationErrors) {
        List<String> errorStrings = new ArrayList<>();

        for (ConfigurationValidationError validationError : validationErrors) {
            errorStrings.add(validationError.getMessage());
        }

        return errorStrings;
    }

    @Override
    public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
        logger.info("Registered framework with frameworkId: " + frameworkId.getValue());
        try {
            initialize();
            stateStore.storeFrameworkId(frameworkId);
        } catch (Exception e) {
            logger.error("Unable to startup at registration time with exception: ", e);
            hardExit(SchedulerErrorCode.REGISTRATION_FAILED);
        }
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
        logger.error("Re-registered framework with master: " + masterInfo);
        hardExit(SchedulerErrorCode.REREGISTERED);
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        logOffers(offers);
        reconciler.reconcile(driver);

        List<Protos.OfferID> acceptedOffers = new ArrayList<>();

        if (!reconciler.isReconciled()) {
            logger.info("Accepting no offers: Reconciler is still in progress");
        } else {
            Block block = stageManager.getCurrentBlock();
            if (block != null) {
                acceptedOffers = stageScheduler.resourceOffers(driver, offers, block);
            }
        }

        SchedulerUtils.declineOffers(driver, acceptedOffers, offers);
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {

    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {

    }

    @Override
    public void frameworkMessage(
            SchedulerDriver driver,
            Protos.ExecutorID executorId,
            Protos.SlaveID slaveId,
            byte[] data) {

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
