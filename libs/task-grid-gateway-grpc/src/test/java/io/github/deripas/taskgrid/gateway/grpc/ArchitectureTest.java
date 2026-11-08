package io.github.deripas.taskgrid.gateway.grpc;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

/**
 * Architecture rules of task-grid-gateway-grpc, checked by the regular build ({@code make verify}).
 * <pre>
 *   grpc   GatewayService, ResultStream - adapter: protocol v1 &lt;-&gt; Gateway and TaskCodec of task-grid-core
 *     ↓
 *   v1     classes generated from gateway.proto; seen only by grpc
 * </pre>
 * The application gives the codec, registers MailboxSink on the queue and GatewayService in its gRPC server.
 * A rule gets in the way - change it here explicitly, with a reason next to it; do not work around it.
 */
@AnalyzeClasses(packages = ArchitectureTest.BASE, importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {
    static final String BASE = "io.github.deripas.taskgrid.gateway.grpc";
    private static final String CORE = "io.github.deripas.taskgrid";

    @ArchTest
    static final ArchRule KNOWN_PACKAGES = classes().should().resideInAnyPackage(BASE, BASE + ".v1..");

    /**
     * The adapter: gRPC, protobuf, its protocol and the API of the core - the gateway, the mailbox, the types.
     */
    @ArchTest
    static final ArchRule ADAPTER_DEPENDENCIES = classes().that().resideInAPackage(BASE)
        .should().onlyDependOnClassesThat().resideInAnyPackage(
            "java..", "org.slf4j..", "lombok..", "io.grpc..", "com.google.protobuf..", "com.google.common..",
            CORE + ".gateway", CORE + ".mailbox", CORE + ".task", BASE, BASE + ".v1..");
}
