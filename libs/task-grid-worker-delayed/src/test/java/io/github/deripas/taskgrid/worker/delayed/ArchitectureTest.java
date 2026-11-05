package io.github.deripas.taskgrid.worker.delayed;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

/**
 * Architecture rules of task-grid-worker-delayed, checked by the regular build ({@code make verify}).
 * <pre>
 *   delayed  DelayedWorker (decorator with a timer), port DelayPolicy, FixedDelay - one package
 *     ↓
 *   core     task, worker of task-grid-core: it wraps any Worker; queue - only the port package, for
 *            TaskQueue.PARTITION_KEY (the pause is per partition of the queue); not the implementations, not result
 * </pre>
 * A rule gets in the way - change it here explicitly, with a reason next to it; do not work around it.
 */
@AnalyzeClasses(packages = ArchitectureTest.BASE, importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {
    static final String BASE = "io.github.deripas.taskgrid.worker.delayed";
    private static final String CORE = "io.github.deripas.taskgrid";

    /**
     * {@code queue} (the package of the port, not {@code queue.memory}) - for the partition key: the decorator holds
     * back the next attempt of a partition of the queue.
     */
    @ArchTest
    static final ArchRule ONLY_THE_PORTS_OF_THE_CORE = classes().that().resideInAPackage(BASE + "..")
        .should().onlyDependOnClassesThat().resideInAnyPackage(
            "java..", "org.slf4j..", "lombok..", CORE + ".task", CORE + ".worker", CORE + ".queue", BASE);

    @ArchTest
    static final ArchRule ONE_PACKAGE = classes().that().resideInAPackage(BASE + "..")
        .should().resideInAPackage(BASE);
}
