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
 *   queue   TaskQueue: keeps tasks per affinity key, assigns attempts to the worker, puts final results
 *         ↓                    ↓
 *   worker  Worker, AttemptCallback     result  ResultStore
 *         ↓                    ↓
 *   task    Task, Attempt, statuses, results, Blob - pure types
 * </pre>
 * worker and result do not know each other. The whole module depends on the JDK only.
 * A rule gets in the way - change it here explicitly, with a reason next to it; do not work around it.
 */
@AnalyzeClasses(packages = "io.github.deripas.taskgrid", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {
    private static final String BASE = "io.github.deripas.taskgrid";

    @ArchTest
    static final ArchRule LAYERS = layeredArchitecture()
        .consideringOnlyDependenciesInLayers()
        .ensureAllClassesAreContainedInArchitecture()
        .layer("task").definedBy(BASE + ".task..")
        .layer("worker").definedBy(BASE + ".worker..")
        .layer("result").definedBy(BASE + ".result..")
        .layer("queue").definedBy(BASE + ".queue..")
        .whereLayer("queue").mayNotBeAccessedByAnyLayer()
        .whereLayer("worker").mayOnlyBeAccessedByLayers("queue")
        .whereLayer("result").mayOnlyBeAccessedByLayers("queue")
        .whereLayer("task").mayOnlyBeAccessedByLayers("queue", "worker", "result");

    /**
     * Core is reusable in any environment: no frameworks, no libraries.
     */
    @ArchTest
    static final ArchRule JDK_ONLY = classes()
        .should().onlyDependOnClassesThat().resideInAnyPackage("java..", BASE + "..");

    @ArchTest
    static final ArchRule NO_CYCLES = slices().matching(BASE + ".(*)..").should().beFreeOfCycles();
}
