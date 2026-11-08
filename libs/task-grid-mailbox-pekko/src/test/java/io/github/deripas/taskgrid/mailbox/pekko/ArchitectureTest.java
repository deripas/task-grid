package io.github.deripas.taskgrid.mailbox.pekko;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Architecture rules of task-grid-mailbox-pekko, checked by the regular build ({@code make verify}).
 * <pre>
 *   pekko   PekkoMailbox (port Mailbox), BoxEntity (sharded event-sourced box), BoxSubscriber (a subscription on the
 *           node of its subscriber)
 *     ↓
 *   core    task, mailbox of task-grid-core (not its implementations)
 * </pre>
 * Jackson for the types of the core - task-grid-pekko-core. The application creates the actor system (cluster,
 * journal) and registers MailboxSink on its queue.
 * A rule gets in the way - change it here explicitly, with a reason next to it; do not work around it.
 */
@AnalyzeClasses(packages = ArchitectureTest.BASE, importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {
    static final String BASE = "io.github.deripas.taskgrid.mailbox.pekko";
    private static final String CORE = "io.github.deripas.taskgrid";

    @ArchTest
    static final ArchRule ENGINE_DEPENDENCIES = classes().that().resideInAPackage(BASE)
        .should().onlyDependOnClassesThat().resideInAnyPackage(
            "java..", "org.slf4j..", "lombok..", "org.apache.pekko..", "com.typesafe.config..", "scala..",
            CORE + ".task", CORE + ".mailbox", BASE);

    @ArchTest
    static final ArchRule NO_JACKSON = noClasses().should().dependOnClassesThat().resideInAnyPackage("com.fasterxml.jackson..");
}
