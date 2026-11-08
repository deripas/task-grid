package io.github.deripas.taskgrid.pekko;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

/**
 * Architecture rules of task-grid-pekko-core, checked by the regular build ({@code make verify}).
 * <pre>
 *   serialization  TaskGridJacksonModule - Jackson for the types of the core; found by Pekko through reference.conf
 *     ↓
 *   core           task of task-grid-core
 * </pre>
 * A rule gets in the way - change it here explicitly, with a reason next to it; do not work around it.
 */
@AnalyzeClasses(packages = ArchitectureTest.BASE, importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {
    static final String BASE = "io.github.deripas.taskgrid.pekko";

    @ArchTest
    static final ArchRule KNOWN_PACKAGES = classes().should().resideInAnyPackage(BASE + ".serialization");

    @ArchTest
    static final ArchRule SERIALIZATION_DEPENDENCIES = classes().that().resideInAPackage(BASE + ".serialization")
        .should().onlyDependOnClassesThat().resideInAnyPackage(
            "java..", "com.fasterxml.jackson..", "io.github.deripas.taskgrid.task", BASE + ".serialization");
}
