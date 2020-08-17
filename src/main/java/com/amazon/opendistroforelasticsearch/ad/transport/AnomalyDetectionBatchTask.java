package com.amazon.opendistroforelasticsearch.ad.transport;

import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;

public class AnomalyDetectionBatchTask extends CancellableTask {

    public AnomalyDetectionBatchTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
        super(id, type, action, description, parentTaskId, headers);
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return true;
    }


}
