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
 *   queue.memory   result.memory   worker.local     implementations: see only ports, nobody sees them
 *         ↓                                         (worker.local: Handler)
 *   queue.partition  Partition, PartitionEvent - event-sourced state machine of a partition, shared by the queue
 *                    engines (in-memory here, Pekko in its own library)
 *   dispatch       Dispatcher - connects a queue to a worker through their ports, nobody sees it
 *
 *   queue    TaskQueue, DelayPolicy, callbacks      worker   Worker, AttemptCallback, Assignment
 *     ↓
 *   result   ResultStore
 *
 *   task     Task, Attempt, statuses, results, Blob - pure types, seen by all
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
        .layer("result").definedBy(BASE + ".result")
        .layer("queue").definedBy(BASE + ".queue")
        .layer("queue.partition").definedBy(BASE + ".queue.partition..")
        .layer("dispatch").definedBy(BASE + ".dispatch..")
        .layer("worker.local").definedBy(BASE + ".worker.local..")
        .layer("result.memory").definedBy(BASE + ".result.memory..")
        .layer("queue.memory").definedBy(BASE + ".queue.memory..")
        .whereLayer("queue.memory").mayNotBeAccessedByAnyLayer()
        .whereLayer("result.memory").mayNotBeAccessedByAnyLayer()
        .whereLayer("worker.local").mayNotBeAccessedByAnyLayer()
        .whereLayer("dispatch").mayNotBeAccessedByAnyLayer()
        .whereLayer("queue").mayOnlyBeAccessedByLayers("queue.memory", "queue.partition", "dispatch")
        // public: the engines of other libraries (Pekko) run the same state machine
        .whereLayer("queue.partition").mayOnlyBeAccessedByLayers("queue.memory")
        .whereLayer("result").mayOnlyBeAccessedByLayers("queue", "queue.memory", "result.memory")
        .whereLayer("worker").mayOnlyBeAccessedByLayers("worker.local", "dispatch")
        .whereLayer("task").mayOnlyBeAccessedByLayers(
            "queue", "worker", "result", "dispatch", "queue.partition", "queue.memory", "result.memory", "worker.local");

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
