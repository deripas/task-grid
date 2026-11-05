package io.github.deripas.crawler.urlmanager;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.Architectures.layeredArchitecture;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

/**
 * Architecture rules of url-manager, checked by the regular build ({@code make verify}).
 * <pre>
 *   app      UrlManagerApplication, UrlManagerConfiguration - assembly: Spring, task-grid node (in memory or in the Pekko
 *            cluster: PekkoCluster), adapters; nobody sees it
 *     ↓
 *   api      FetchController - inbound adapter: HTTP (Spring MVC) &lt;-&gt; fetch
 *   amqp     FetchMessages, RabbitFetchListener - inbound adapter: RabbitMQ (Spring AMQP), url_manager.proto (v1,
 *            from url-manager-api) &lt;-&gt; fetch; acknowledgement after the reply
 *   grpc     NodeServer - inbound adapter: gRPC server of the node for sidecars (NodeService of task-grid)
 *   metrics  PoolMetrics - outbound adapter: load of the remote worker by kind -&gt; Micrometer (autoscaling)
 *   fetcher  ProtoPageCodec - outbound adapter: fetcher.proto (from fetcher-api) implements the port PageCodec
 *   blob     S3BlobReader - outbound adapter: S3 (AWS SDK) and zstd implement the port BlobReader
 *   config   UrlManagerProperties - settings, data only
 *     ↓
 *   fetch    FetchService, PageUrl, Page, FetchOutcome, ports PageCodec and BlobReader - domain and use case on the
 *            API of task-grid (task, queue, result, worker), not on its implementations
 * </pre>
 * The adapters do not know each other. A rule gets in the way - change it here explicitly, with a reason next to it.
 */
@AnalyzeClasses(packages = ArchitectureTest.BASE, importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {
    static final String BASE = "io.github.deripas.crawler.urlmanager";
    private static final String TASK_GRID = "io.github.deripas.taskgrid";

    @ArchTest
    static final ArchRule LAYERS = layeredArchitecture()
        .consideringOnlyDependenciesInLayers()
        .ensureAllClassesAreContainedInArchitecture()
        .layer("app").definedBy(BASE + ".app..")
        .layer("api").definedBy(BASE + ".api..")
        .layer("amqp").definedBy(BASE + ".amqp..")
        .layer("v1").definedBy(BASE + ".v1..")
        .layer("grpc").definedBy(BASE + ".grpc..")
        .layer("metrics").definedBy(BASE + ".metrics..")
        .layer("fetcher").definedBy(BASE + ".fetcher..")
        .layer("blob").definedBy(BASE + ".blob..")
        .layer("config").definedBy(BASE + ".config..")
        .layer("fetch").definedBy(BASE + ".fetch..")
        .whereLayer("app").mayNotBeAccessedByAnyLayer()
        .whereLayer("api").mayOnlyBeAccessedByLayers("app")
        .whereLayer("amqp").mayOnlyBeAccessedByLayers("app")
        .whereLayer("v1").mayOnlyBeAccessedByLayers("amqp")
        .whereLayer("grpc").mayOnlyBeAccessedByLayers("app")
        .whereLayer("metrics").mayOnlyBeAccessedByLayers("app")
        .whereLayer("fetcher").mayOnlyBeAccessedByLayers("app")
        .whereLayer("blob").mayOnlyBeAccessedByLayers("app")
        .whereLayer("config").mayOnlyBeAccessedByLayers("app")
        .whereLayer("fetch").mayOnlyBeAccessedByLayers("app", "api", "amqp", "fetcher", "blob");

    /**
     * Domain and use case: JDK, lombok, slf4j and the API of task-grid; neither protocols nor frameworks nor storage.
     */
    @ArchTest
    static final ArchRule FETCH_DEPENDENCIES = classes().that().resideInAPackage(BASE + ".fetch..")
        .should().onlyDependOnClassesThat().resideInAnyPackage(
            "java..", "lombok..", "org.slf4j..",
            TASK_GRID + ".task..", TASK_GRID + ".queue", TASK_GRID + ".result", TASK_GRID + ".worker",
            BASE + ".fetch..");

    /**
     * Implementations of task-grid (memory, remote) and their wiring (dispatch) are chosen by the assembly; the gRPC
     * server only hosts NodeService; metrics read the load of the remote worker - it is the remote worker that knows
     * its waiting attempts and slots.
     */
    @ArchTest
    static final ArchRule TASK_GRID_IMPLEMENTATIONS_ONLY_IN_APP_AND_GRPC = noClasses()
        .that().resideOutsideOfPackages(BASE + ".app..", BASE + ".grpc..", BASE + ".metrics..")
        .should().dependOnClassesThat().resideInAnyPackage(
            TASK_GRID + ".queue.memory..", TASK_GRID + ".result.memory..", TASK_GRID + ".worker.remote..", TASK_GRID + ".worker.local..",
            TASK_GRID + ".dispatch..", TASK_GRID + ".queue.pekko..");

    /**
     * The cluster is a matter of the assembly: the use case and the adapters see only the TaskQueue port.
     */
    @ArchTest
    static final ArchRule PEKKO_ONLY_IN_APP = noClasses()
        .that().resideOutsideOfPackage(BASE + ".app..")
        .should().dependOnClassesThat().resideInAnyPackage("org.apache.pekko..", "com.typesafe.config..");

    @ArchTest
    static final ArchRule NO_SPRING_IN_DOMAIN_AND_OUTBOUND_ADAPTERS = noClasses()
        .that().resideInAnyPackage(BASE + ".fetch..", BASE + ".fetcher..", BASE + ".blob..")
        .should().dependOnClassesThat().resideInAnyPackage("org.springframework..", "jakarta..");

    @ArchTest
    static final ArchRule FETCHER_PROTOCOL_ONLY_IN_FETCHER = noClasses()
        .that().resideOutsideOfPackage(BASE + ".fetcher..")
        .should().dependOnClassesThat().resideInAnyPackage("io.github.deripas.crawler.fetcher..");

    @ArchTest
    static final ArchRule GRPC_ONLY_IN_FETCHER_AND_GRPC = noClasses()
        .that().resideOutsideOfPackages(BASE + ".fetcher..", BASE + ".grpc..")
        .should().dependOnClassesThat().resideInAnyPackage("io.grpc..");

    /**
     * Generated classes of both protocols are protobuf; the fetch domain sees neither.
     */
    @ArchTest
    static final ArchRule PROTOBUF_ONLY_IN_PROTOCOL_ADAPTERS = noClasses()
        .that().resideOutsideOfPackages(BASE + ".fetcher..", BASE + ".grpc..", BASE + ".amqp..", BASE + ".v1..")
        .should().dependOnClassesThat().resideInAnyPackage("com.google.protobuf..");

    @ArchTest
    static final ArchRule RABBIT_ONLY_IN_AMQP_AND_APP = noClasses()
        .that().resideOutsideOfPackages(BASE + ".amqp..", BASE + ".app..")
        .should().dependOnClassesThat().resideInAnyPackage("org.springframework.amqp..", "com.rabbitmq..");

    @ArchTest
    static final ArchRule STORAGE_ONLY_IN_BLOB_AND_APP = noClasses()
        .that().resideOutsideOfPackages(BASE + ".blob..", BASE + ".app..")
        .should().dependOnClassesThat().resideInAnyPackage("software.amazon..", "com.github.luben..");

    @ArchTest
    static final ArchRule NO_CYCLES = slices().matching(BASE + ".(**)").should().beFreeOfCycles();
}
