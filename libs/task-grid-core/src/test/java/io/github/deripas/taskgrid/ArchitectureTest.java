package io.github.deripas.taskgrid;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.library.Architectures.layeredArchitecture;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

/**
 * Architecture rules of task-grid-core, checked by the regular build ({@code make verify}).
 * <pre>
 *   gateway  Gateway, TaskCodec - the entry of clients: reserve in the box, submit, wait on this node; the transports
 *            (gRPC, AMQP) are libraries over it; nobody sees it
 *         ↓
 *   queue.memory   mailbox.memory   worker.local     implementations: see only ports, nobody sees them
 *         ↓                                           (worker.local: Handler)
 *   queue.partition  Partition, PartitionEvent - event-sourced state machine of a partition, shared by the queue
 *                    engines (in-memory here, Pekko in its own library)
 *   dispatch       Dispatcher - connects a queue to a worker through their ports, nobody sees it
 *
 *   queue    TaskQueue, DelayPolicy, callbacks      worker   Worker, AttemptCallback, Assignment
 *
 *   mailbox  Mailbox, Box - results of requests by consumer until acknowledged: the way back to a client on any node;
 *            MailboxSink - a ResultCallback of the queue that fills the boxes
 *
 *   task     Task, Request, Attempt, statuses, results, Blob - pure types, seen by all
 * </pre>
 * queue and worker do not know each other: only dispatch sees both. Implementations are wired together by
 * the application (or a test).
 * A rule gets in the way - change it here explicitly, with a reason next to it; do not work around it.
 */
@AnalyzeClasses(packages = "io.github.deripas.taskgrid", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {
    private static final String BASE = "io.github.deripas.taskgrid";

    @ArchTest
    static final ArchRule LAYERS = layeredArchitecture()
        .consideringOnlyDependenciesInLayers()
        .ensureAllClassesAreContainedInArchitecture()
        .layer("task").definedBy(BASE + ".task")
        .layer("worker").definedBy(BASE + ".worker")
        .layer("queue").definedBy(BASE + ".queue")
        .layer("queue.partition").definedBy(BASE + ".queue.partition..")
        .layer("dispatch").definedBy(BASE + ".dispatch..")
        .layer("worker.local").definedBy(BASE + ".worker.local..")
        .layer("queue.memory").definedBy(BASE + ".queue.memory..")
        .layer("gateway").definedBy(BASE + ".gateway..")
        .whereLayer("gateway").mayNotBeAccessedByAnyLayer()
        .layer("mailbox").definedBy(BASE + ".mailbox")
        .layer("mailbox.memory").definedBy(BASE + ".mailbox.memory..")
        .whereLayer("mailbox.memory").mayNotBeAccessedByAnyLayer()
        .whereLayer("mailbox").mayOnlyBeAccessedByLayers("mailbox.memory", "gateway")
        .whereLayer("queue.memory").mayNotBeAccessedByAnyLayer()
        .whereLayer("worker.local").mayNotBeAccessedByAnyLayer()
        .whereLayer("dispatch").mayNotBeAccessedByAnyLayer()
        .whereLayer("queue").mayOnlyBeAccessedByLayers("queue.memory", "queue.partition", "dispatch", "mailbox", "gateway")
        // public: the engines of other libraries (Pekko) run the same state machine
        .whereLayer("queue.partition").mayOnlyBeAccessedByLayers("queue.memory")
        .whereLayer("worker").mayOnlyBeAccessedByLayers("worker.local", "dispatch")
        .whereLayer("task").mayOnlyBeAccessedByLayers(
            "queue", "worker", "dispatch", "queue.partition", "queue.memory", "worker.local", "mailbox", "mailbox.memory",
            "gateway");

    /**
     * Core is reusable in any environment: the JDK plus a short, explicit list of libraries.
     * slf4j-api - logging facade (lombok @Slf4j); lombok - compile-time only.
     * A new library goes here deliberately, with its reason.
     */
    @ArchTest
    static final ArchRule MINIMAL_DEPENDENCIES = classes()
        .should().onlyDependOnClassesThat().resideInAnyPackage("java..", "org.slf4j..", "lombok..", BASE + "..");

    @ArchTest
    static final ArchRule NO_CYCLES = slices().matching(BASE + ".(**)").should().beFreeOfCycles();
}
