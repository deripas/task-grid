package io.github.deripas.taskgrid.queue.pekko;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.Architectures.layeredArchitecture;

/**
 * Architecture rules of task-grid-queue-pekko, checked by the regular build ({@code make verify}).
 * <pre>
 *   pekko          PekkoTaskQueue (port TaskQueue), PartitionEntity (sharded event-sourced engine of a Partition),
 *                  Receivers (callbacks of the node)
 *     ↓
 *   core           task, queue, queue.partition of task-grid-core (not its implementations, not worker)
 * </pre>
 * Jackson for the types of the core - task-grid-pekko-core.
 * The application creates the actor system (cluster, journal) and connects the queue to a worker (Dispatcher).
 * A rule gets in the way - change it here explicitly, with a reason next to it; do not work around it.
 */
@AnalyzeClasses(packages = ArchitectureTest.BASE, importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {
    static final String BASE = "io.github.deripas.taskgrid.queue.pekko";
    private static final String CORE = "io.github.deripas.taskgrid";

    @ArchTest
    static final ArchRule LAYERS = layeredArchitecture()
        .consideringOnlyDependenciesInLayers()
        .ensureAllClassesAreContainedInArchitecture()
        .layer("pekko").definedBy(BASE)
        .whereLayer("pekko").mayNotBeAccessedByAnyLayer();

    /**
     * The engine: the JDK, slf4j, lombok, Pekko (with its config) and the ports and state machine of the core.
     */
    @ArchTest
    static final ArchRule ENGINE_DEPENDENCIES = classes().that().resideInAPackage(BASE)
        .should().onlyDependOnClassesThat().resideInAnyPackage(
            "java..", "org.slf4j..", "lombok..", "org.apache.pekko..", "com.typesafe.config..", "scala..",
            CORE + ".task", CORE + ".queue", CORE + ".queue.partition", BASE);

    @ArchTest
    static final ArchRule NO_JACKSON = noClasses().should().dependOnClassesThat().resideInAnyPackage("com.fasterxml.jackson..");
}
