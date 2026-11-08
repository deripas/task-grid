package io.github.deripas.taskgrid.pekko.serialization;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.github.deripas.taskgrid.task.Blob;

/**
 * Jackson for the types of task-grid-core, which know nothing of Jackson: mix-ins instead of annotations. Registered
 * in {@code pekko.serialization.jackson.jackson-modules} (reference.conf).
 * <p>
 * Names of the subtypes are part of the journal format: do not rename.
 */
public final class TaskGridJacksonModule extends SimpleModule {
    public TaskGridJacksonModule() {
        super("task-grid");
        setMixInAnnotation(Blob.class, BlobMixIn.class);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = Blob.Inline.class, name = "inline"),
        @JsonSubTypes.Type(value = Blob.Ref.class, name = "ref")
    })
    private interface BlobMixIn {
    }
}
