package com.amazon.opendistroforelasticsearch.ad.common.exception;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.IOException;

public class ADTaskCancelledException extends TaskCancelledException {
    private String cancelledBy;

    public ADTaskCancelledException(String msg) {
        super(msg);
    }

    public ADTaskCancelledException(String msg, String user) {
        super(msg);
        this.cancelledBy = user;
    }

    public ADTaskCancelledException(StreamInput in) throws IOException {
        super(in);
        this.cancelledBy = in.readOptionalString();
    }

    public String getCancelledBy() {
        return cancelledBy;
    }

    public void setCancelledBy(String cancelledBy) {
        this.cancelledBy = cancelledBy;
    }
}
