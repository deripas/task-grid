package io.github.deripas.taskgrid.worker.remote;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.library.Architectures.layeredArchitecture;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

/**
 * Architecture rules of task-grid-worker-remote, checked by the regular build ({@code make verify}).
 * <pre>
 *   grpc     NodeService, StreamChannel - adapter: protocol v1 &lt;-&gt; ports of remote; nobody sees it
 *     ↓   ↘
 *     ↓    v1   classes generated from remote.proto; seen only by grpc
 *     ↓
 *   remote   RemoteWorker (engine), Allocator (state machine), ports Sidecars / Session / SessionChannel
 *     ↓
 *   core     task, worker of task-grid-core (not queue, not result)
 * </pre>
 * The application wires RemoteWorker into a queue and registers NodeService in its gRPC server.
 * A rule gets in the way - change it here explicitly, with a reason next to it; do not work around it.
 */
@AnalyzeClasses(packages = ArchitectureTest.BASE, importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {
    static final String BASE = "io.github.deripas.taskgrid.worker.remote";
    private static final String CORE = "io.github.deripas.taskgrid";

    @ArchTest
    static final ArchRule LAYERS = layeredArchitecture()
        .consideringOnlyDependenciesInLayers()
        .ensureAllClassesAreContainedInArchitecture()
        .layer("remote").definedBy(BASE)
        .layer("grpc").definedBy(BASE + ".grpc..")
        .layer("v1").definedBy(BASE + ".v1..")
        .whereLayer("grpc").mayNotBeAccessedByAnyLayer()
        .whereLayer("v1").mayOnlyBeAccessedByLayers("grpc")
        .whereLayer("remote").mayOnlyBeAccessedByLayers("grpc");

    /**
     * The engine and the state machine know neither the protocol nor gRPC: the JDK, slf4j, lombok and the ports
     * of the core they implement or call.
     */
    @ArchTest
    static final ArchRule REMOTE_DEPENDENCIES = classes().that().resideInAPackage(BASE)
        .should().onlyDependOnClassesThat().resideInAnyPackage(
            "java..", "org.slf4j..", "lombok..", CORE + ".task", CORE + ".worker", BASE);

    @ArchTest
    static final ArchRule NO_CYCLES = slices().matching(BASE + ".(**)").should().beFreeOfCycles();
}
