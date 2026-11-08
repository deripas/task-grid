package io.github.deripas.taskgrid.gateway.amqp;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

/**
 * Architecture rules of task-grid-gateway-amqp, checked by the regular build ({@code make verify}).
 * <pre>
 *   amqp   AmqpGateway (consumer, ConfirmedPublisher), AckAfterReply, AckAfterSubmit - adapter: RabbitMQ (amqp-client)
 *     ↓    &lt;-&gt; Gateway, TaskCodec, TaskQueue of task-grid-core
 *   core   gateway, queue, task of task-grid-core
 * </pre>
 * No Spring: the application gives the connection factory and starts / closes the gateway.
 * A rule gets in the way - change it here explicitly, with a reason next to it; do not work around it.
 */
@AnalyzeClasses(packages = ArchitectureTest.BASE, importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {
    static final String BASE = "io.github.deripas.taskgrid.gateway.amqp";
    private static final String CORE = "io.github.deripas.taskgrid";

    @ArchTest
    static final ArchRule ADAPTER_DEPENDENCIES = classes().should().onlyDependOnClassesThat().resideInAnyPackage(
        "java..", "org.slf4j..", "lombok..", "com.rabbitmq.client..",
        CORE + ".gateway", CORE + ".queue", CORE + ".task", BASE);
}
