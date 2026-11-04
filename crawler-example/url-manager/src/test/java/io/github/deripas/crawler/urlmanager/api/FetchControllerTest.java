package io.github.deripas.crawler.urlmanager.api;

import io.github.deripas.crawler.urlmanager.fetch.LocalFetch;
import io.github.deripas.taskgrid.task.Blob;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * HTTP of {@code POST /fetch} over a FetchService with a handler in the JVM.
 */
class FetchControllerTest {
    private LocalFetch fetch;
    private MockMvc mvc;

    @BeforeEach
    void start() {
        fetch = new LocalFetch(attempt -> {
            final String url = LocalFetch.url(attempt);
            if (url.endsWith("/down")) {
                throw new IllegalStateException("FailedPrecondition: site unreachable");
            }
            if (url.endsWith("/slow")) {
                Thread.sleep(2_000);
            }
            return Blob.of(("page of " + url).getBytes(StandardCharsets.UTF_8));
        }, ref -> {
            throw new IOException("no blobs");
        });
        final FetchController controller = new FetchController(fetch.service(), Duration.ofSeconds(5), Duration.ofSeconds(10));
        mvc = MockMvcBuilders.standaloneSetup(controller).setControllerAdvice(new Problems()).build();
    }

    @AfterEach
    void stop() {
        fetch.close();
    }

    @Test
    void pageIsTheBodyOfTheResponse() throws Exception {
        mvc.perform(fetch("{\"url\": \"https://example.com/a\"}"))
            .andExpect(status().isOk())
            .andExpect(header().string(FetchController.FETCH_STATUS, "200"))
            .andExpect(content().contentType(MediaType.APPLICATION_OCTET_STREAM))
            .andExpect(content().string("page of https://example.com/a"));
    }

    @Test
    void failedFetchIsBadGateway() throws Exception {
        mvc.perform(fetch("{\"url\": \"https://example.com/down\"}"))
            .andExpect(status().isBadGateway())
            .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
            .andExpect(jsonPath("$.detail").value(containsString("site unreachable")));
    }

    @Test
    void noPageWithinTheTimeoutIsGatewayTimeout() throws Exception {
        mvc.perform(fetch("{\"url\": \"https://example.com/slow\", \"timeoutMs\": 100}"))
            .andExpect(status().isGatewayTimeout());
    }

    @Test
    void invalidUrlIsBadRequest() throws Exception {
        mvc.perform(fetch("{\"url\": \"ftp://example.com/a\"}"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.detail").value("not an absolute http(s) URL: ftp://example.com/a"));
    }

    @Test
    void nonPositiveTimeoutIsBadRequest() throws Exception {
        mvc.perform(fetch("{\"url\": \"https://example.com/a\", \"timeoutMs\": 0}"))
            .andExpect(status().isBadRequest());
    }

    private static RequestBuilder fetch(String json) {
        return post("/fetch").contentType(MediaType.APPLICATION_JSON).content(json);
    }

    /**
     * Problem details, as {@code spring.mvc.problemdetails.enabled} gives the application.
     */
    @ControllerAdvice
    static final class Problems extends ResponseEntityExceptionHandler {
    }
}
