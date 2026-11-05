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
 *   app      UrlManagerApplication, UrlManagerConfiguration - assembly: Spring, task-grid node, adapters; nobody sees it
 *     ↓
 *   api      FetchController - inbound adapter: HTTP (Spring MVC) &lt;-&gt; fetch
 *   grpc     NodeServer - inbound adapter: gRPC server of the node for sidecars (NodeService of task-grid)
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
        .layer("grpc").definedBy(BASE + ".grpc..")
        .layer("fetcher").definedBy(BASE + ".fetcher..")
        .layer("blob").definedBy(BASE + ".blob..")
        .layer("config").definedBy(BASE + ".config..")
        .layer("fetch").definedBy(BASE + ".fetch..")
        .whereLayer("app").mayNotBeAccessedByAnyLayer()
        .whereLayer("api").mayOnlyBeAccessedByLayers("app")
        .whereLayer("grpc").mayOnlyBeAccessedByLayers("app")
        .whereLayer("fetcher").mayOnlyBeAccessedByLayers("app")
        .whereLayer("blob").mayOnlyBeAccessedByLayers("app")
        .whereLayer("config").mayOnlyBeAccessedByLayers("app")
        .whereLayer("fetch").mayOnlyBeAccessedByLayers("app", "api", "fetcher", "blob");

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
     * Implementations of task-grid (memory, remote) are chosen by the assembly; the gRPC server only hosts NodeService.
     */
    @ArchTest
    static final ArchRule TASK_GRID_IMPLEMENTATIONS_ONLY_IN_APP_AND_GRPC = noClasses()
        .that().resideOutsideOfPackages(BASE + ".app..", BASE + ".grpc..")
        .should().dependOnClassesThat().resideInAnyPackage(
            TASK_GRID + ".queue.memory..", TASK_GRID + ".result.memory..", TASK_GRID + ".worker.remote..", TASK_GRID + ".worker.local..",
            TASK_GRID + ".worker.delayed..");

    @ArchTest
    static final ArchRule SPRING_ONLY_IN_APP_API_CONFIG_GRPC = noClasses()
        .that().resideInAnyPackage(BASE + ".fetch..", BASE + ".fetcher..", BASE + ".blob..")
        .should().dependOnClassesThat().resideInAnyPackage("org.springframework..", "jakarta..");

    @ArchTest
    static final ArchRule PROTOCOLS_ONLY_IN_FETCHER_AND_GRPC = noClasses()
        .that().resideOutsideOfPackages(BASE + ".fetcher..", BASE + ".grpc..")
        .should().dependOnClassesThat().resideInAnyPackage("io.grpc..", "com.google.protobuf..", "io.github.deripas.crawler.fetcher..");

    @ArchTest
    static final ArchRule STORAGE_ONLY_IN_BLOB_AND_APP = noClasses()
        .that().resideOutsideOfPackages(BASE + ".blob..", BASE + ".app..")
        .should().dependOnClassesThat().resideInAnyPackage("software.amazon..", "com.github.luben..");

    @ArchTest
    static final ArchRule NO_CYCLES = slices().matching(BASE + ".(**)").should().beFreeOfCycles();
}
