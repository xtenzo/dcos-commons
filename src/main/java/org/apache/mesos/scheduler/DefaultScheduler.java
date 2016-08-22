package org.apache.mesos.scheduler;

import com.google.protobuf.TextFormat;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.config.*;
import org.apache.mesos.curator.CuratorConfigStore;
import org.apache.mesos.curator.CuratorStateStore;
import org.apache.mesos.offer.*;
import org.apache.mesos.reconciliation.DefaultReconciler;
import org.apache.mesos.reconciliation.Reconciler;
import org.apache.mesos.scheduler.plan.*;
import org.apache.mesos.scheduler.recovery.*;
import org.apache.mesos.scheduler.recovery.constrain.TimedLaunchConstrainer;
import org.apache.mesos.scheduler.recovery.monitor.TimedFailureMonitor;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.state.StateStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by gabriel on 8/20/16.
 */
public class DefaultScheduler implements Scheduler {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final String frameworkName;
    private final ConfigurationValidator configurationValidator;
    private final DefaultServiceSpecification serviceSpecification;
    private final AtomicReference<RecoveryStatus> recoveryStatusRef;

    private StateStore stateStore;
    private ConfigStore configStore;
    private Reconciler reconciler;
    private StageManager stageManager;
    private OfferAccepter offerAccepter;
    private DefaultStageScheduler stageScheduler;
    private DefaultRecoveryScheduler recoveryScheduler;
    private TaskKiller taskKiller;

    public DefaultScheduler(String frameworkName,
                            DefaultServiceSpecification serviceSpecification,
                            Collection<ConfigurationValidation> validations) {
        this.frameworkName = frameworkName;
        this.serviceSpecification = serviceSpecification;
        this.configurationValidator = new DefaultConfigurationValidator(validations);
        this.recoveryStatusRef = new AtomicReference<>(
                new RecoveryStatus(Collections.emptyList(), Collections.emptyList()));
    }

    public DefaultScheduler(String frameworkName, DefaultServiceSpecification serviceSpecification) {
        this(frameworkName, serviceSpecification, Collections.emptyList());
    }

    private void initialize() throws ConfigStoreException, TaskException {
        this.stateStore = new CuratorStateStore(frameworkName);
        this.configStore = new CuratorConfigStore<EnvironmentConfiguration>(frameworkName);
        this.reconciler = new DefaultReconciler(stateStore);

        Collection<ConfigurationValidationError> validationErrors = processConfigurationUpdate();

        this.stageManager = new DefaultStageManager(getStage(validationErrors), new DefaultStrategyFactory());
        this.offerAccepter = new OfferAccepter(Arrays.asList(new DefaultOperationRecorder(stateStore)));
        this.stageScheduler = new DefaultStageScheduler(offerAccepter);
        TaskFailureListener taskFailureListener = new DefaultFailureListener(stateStore);
        this.recoveryScheduler = new DefaultRecoveryScheduler(
                stateStore,
                taskFailureListener,
                new DefaultRecoveryRequirementProvider(),
                offerAccepter,
                new TimedLaunchConstrainer(Duration.ofSeconds(600)),
                new TimedFailureMonitor(Duration.ofSeconds(600)),
                recoveryStatusRef);
        this.taskKiller = new DefaultTaskKiller(taskFailureListener);
    }

    private Collection<ConfigurationValidationError> processConfigurationUpdate() throws ConfigStoreException {
        Optional<Configuration> oldConfiguration = getOldConfiguration();
        Configuration newConfiguration = getNewConfiguration();
        Collection<ConfigurationValidationError> configurationValidationErrors =
                configurationValidator.validate(oldConfiguration, newConfiguration);

        Configuration currentConfiguration = configurationValidationErrors.isEmpty() ?
                newConfiguration : oldConfiguration.get();

        UUID currentConfigurationId = configStore.store(currentConfiguration);
        configStore.setTargetConfig(currentConfigurationId);

        return configurationValidationErrors;
    }

    private Stage getStage(Collection<ConfigurationValidationError> validationErrors)
            throws ConfigStoreException, TaskException {

        List<Phase> phases = new ArrayList(Arrays.asList(ReconciliationPhase.create(reconciler)));
        phases.addAll(getDeploymentPhases());

        // If config validation had errors, expose them via the Stage.
        Stage stage = validationErrors.isEmpty()
                ? DefaultStage.fromList(phases)
                : DefaultStage.withErrors(phases, validationErrorsToStrings(validationErrors));

        return stage;
    }

    private List<Phase> getDeploymentPhases() throws TaskException, ConfigStoreException {
        List<Phase> phases = new ArrayList<>();

        for (TaskSpecificationTemplate taskSpecificationTemplate :
                serviceSpecification.getTaskSpecificationTempaltes()) {

            TaskSpecificationFactory taskSpecificationFactory = new DefaultTaskSpecificationFactory();
            List<TaskSpecification> taskSpecifications = taskSpecificationFactory.create(
                    taskSpecificationTemplate,
                    new DefaultConfigurationUpdateDetector(),
                    new EnvironmentConfiguration.Factory(),
                    configStore,
                    stateStore);

            phases.add(DefaultDeploymentPhase.create("deployment", taskKiller, taskSpecifications, stateStore));
        }

        return phases;
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

            List<Protos.Offer> unacceptedOffers = SchedulerUtils.filterAcceptedOffers(offers, acceptedOffers);
            try {
                acceptedOffers.addAll(recoveryScheduler.resourceOffers(driver, unacceptedOffers, block));
            } catch (Exception e) {
                logger.error("Error repairing block: " + block + " Reason: ", e);
            }

            ResourceCleanerScheduler cleanerScheduler = getCleanerScheduler();
            if (cleanerScheduler != null) {
                acceptedOffers.addAll(getCleanerScheduler().resourceOffers(driver, offers));
            }
        }

        SchedulerUtils.declineOffers(driver, acceptedOffers, offers);
    }

    private ResourceCleanerScheduler getCleanerScheduler() {
        try {
            ResourceCleaner cleaner = new ResourceCleaner(stateStore.getExpectedResources());
            return new ResourceCleanerScheduler(cleaner, offerAccepter);
        } catch (Exception ex) {
            logger.error("Failed to construct ResourceCleaner", ex);
            return null;
        }
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {

    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
        stageManager.update(status);
        updateStateStore(status);
    }

    private void updateStateStore(Protos.TaskStatus status) {
        if (!status.getState().equals(Protos.TaskState.TASK_STAGING)
                && !taskStatusExists(status)) {
            logger.warn("Dropping non-STAGING status update because the ZK path doesn't exist: " + status);
        } else {
            stateStore.storeStatus(status);
        }
    }

    private boolean taskStatusExists(Protos.TaskStatus taskStatus) throws StateStoreException {
        String taskName;
        try {
            taskName = TaskUtils.toTaskName(taskStatus.getTaskId());
        } catch (TaskException e) {
            throw new StateStoreException(String.format(
                    "Failed to get TaskName/ExecName from TaskStatus %s", taskStatus), e);
        }
        try {
            stateStore.fetchStatus(taskName);
            return true;
        } catch (Exception e) {
            return false;
        }
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
