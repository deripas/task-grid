package io.github.deripas.crawler.urlmanager.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * url-manager: {@code POST /fetch} on a task-grid node; the fetcher works behind handler-sidecar.
 * Components are not scanned: {@link UrlManagerConfiguration} assembles them.
 */
@SpringBootApplication
public class UrlManagerApplication {
    public static void main(String[] args) {
        SpringApplication.run(UrlManagerApplication.class, args);
    }
}
