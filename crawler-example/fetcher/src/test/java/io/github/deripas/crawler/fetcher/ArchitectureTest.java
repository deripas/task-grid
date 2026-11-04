package io.github.deripas.crawler.fetcher;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.Architectures.layeredArchitecture;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

/**
 * Architecture rules of the fetcher, checked by the regular build ({@code make verify}).
 * <pre>
 *   app      Main - assembly: settings, adapters, gRPC server; nobody sees it
 *     ↓
 *   grpc     FetcherService - adapter: fetcher.proto (v1, from fetcher-api) &lt;-&gt; fetch
 *   http     ApacheFetcher - adapter: Apache HttpClient implements the port Fetcher
 *   config   FetcherSettings - settings from the environment, data only
 *     ↓
 *   fetch    Fetcher (port), TargetUrl, Page, FetchException - domain, JDK only
 * </pre>
 * The adapters do not know each other. A rule gets in the way - change it here explicitly, with a reason next to it.
 */
@AnalyzeClasses(packages = ArchitectureTest.BASE, importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {
    static final String BASE = "io.github.deripas.crawler.fetcher";

    @ArchTest
    static final ArchRule LAYERS = layeredArchitecture()
        .consideringOnlyDependenciesInLayers()
        .ensureAllClassesAreContainedInArchitecture()
        .layer("app").definedBy(BASE + ".app..")
        .layer("grpc").definedBy(BASE + ".grpc..")
        .layer("http").definedBy(BASE + ".http..")
        .layer("config").definedBy(BASE + ".config..")
        .layer("fetch").definedBy(BASE + ".fetch..")
        .layer("v1").definedBy(BASE + ".v1..")
        .whereLayer("app").mayNotBeAccessedByAnyLayer()
        .whereLayer("grpc").mayOnlyBeAccessedByLayers("app")
        .whereLayer("http").mayOnlyBeAccessedByLayers("app")
        .whereLayer("config").mayOnlyBeAccessedByLayers("app")
        .whereLayer("v1").mayOnlyBeAccessedByLayers("grpc")
        .whereLayer("fetch").mayOnlyBeAccessedByLayers("app", "grpc", "http");

    /**
     * The domain knows neither protocols nor HTTP libraries: JDK and lombok.
     */
    @ArchTest
    static final ArchRule DOMAIN_DEPENDENCIES = classes().that().resideInAPackage(BASE + ".fetch..")
        .should().onlyDependOnClassesThat().resideInAnyPackage("java..", "lombok..", BASE + ".fetch..");

    @ArchTest
    static final ArchRule HTTP_CLIENT_ONLY_IN_HTTP = noClasses().that().resideOutsideOfPackage(BASE + ".http..")
        .should().dependOnClassesThat().resideInAPackage("org.apache.hc..");

    @ArchTest
    static final ArchRule GRPC_ONLY_IN_GRPC_AND_APP = noClasses().that().resideInAnyPackage(BASE + ".fetch..", BASE + ".http..", BASE + ".config..")
        .should().dependOnClassesThat().resideInAnyPackage("io.grpc..", "com.google.protobuf..");

    @ArchTest
    static final ArchRule NO_CYCLES = slices().matching(BASE + ".(**)").should().beFreeOfCycles();
}
