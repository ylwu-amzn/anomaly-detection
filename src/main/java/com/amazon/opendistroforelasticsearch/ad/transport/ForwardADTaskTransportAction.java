/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.transport;

import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.ERROR_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.STATE_FIELD;
import static com.amazon.opendistroforelasticsearch.ad.model.ADTask.TASK_PROGRESS_FIELD;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskAction;
import com.amazon.opendistroforelasticsearch.ad.model.ADTaskState;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.task.ADTaskManager;
import com.google.common.collect.ImmutableMap;

public class ForwardADTaskTransportAction extends HandledTransportAction<ForwardADTaskRequest, AnomalyDetectorJobResponse> {
    private final Logger logger = LogManager.getLogger(ForwardADTaskTransportAction.class);
    private final ADTaskManager adTaskManager;
    private final TransportService transportService;

    @Inject
    public ForwardADTaskTransportAction(ActionFilters actionFilters, TransportService transportService, ADTaskManager adTaskManager) {
        super(ForwardADTaskAction.NAME, transportService, actionFilters, ForwardADTaskRequest::new);
        this.adTaskManager = adTaskManager;
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(Task task, ForwardADTaskRequest request, ActionListener<AnomalyDetectorJobResponse> listener) {
        ADTaskAction adTaskAction = request.getAdTaskAction();
        AnomalyDetector detector = request.getDetector();
        String detectorId = detector.getDetectorId();
        ADTask adTask = request.getAdTask();

        switch (adTaskAction) {
            case START:
                logger.debug("Received START action for detector {}", detectorId);
                adTaskManager.startDetector(detector, request.getDetectionDateRange(), request.getUser(), transportService, listener);
                break;
            case FINISHED:
                logger.debug("Received FINISHED action from task {}", adTask.getTaskId());
                adTaskManager.removeDetectorFromCache(detectorId);
                listener.onResponse(new AnomalyDetectorJobResponse(detectorId, 0, 0, 0, RestStatus.OK));
                break;
            case NEXT_ENTITY:
                logger.debug("Received NEXT_ENTITY action from task {}", adTask.getTaskId());
                if (detector.isMultientityDetector()) {
                    adTaskManager.removeRunningEntity(detectorId, adTask.getEntity());
                    if (adTaskManager.hcDetectorDone(detectorId)) {
                        logger.debug("HC historical analysis done, will remove from cache, detector id:{}", detectorId);
                        listener.onResponse(new AnomalyDetectorJobResponse(detectorId, 0, 0, 0, RestStatus.OK));
                        ADTaskState state = !adTask.isEntityTask() && adTask.getError() != null ? ADTaskState.FAILED : ADTaskState.FINISHED;
                        adTaskManager.setHCDetectorTaskDone(adTask, state, adTask.getError(), listener);
                    } else {
                        logger.debug("Run next entity for detector {}", detectorId);
                        adTaskManager.runBatchResultActionForEntity(adTask, listener);

                        float progress = adTaskManager.hcDetectorProgress(detectorId);
                        String error = adTask.getError() != null ? adTask.getError() : "";
                        adTaskManager
                            .updateADHCDetectorTask(
                                detectorId,
                                adTask.getParentTaskId(),
                                ImmutableMap.of(STATE_FIELD, ADTaskState.RUNNING.name(), TASK_PROGRESS_FIELD, progress, ERROR_FIELD, error)
                            );
                    }
                } else {
                    listener.onFailure(new IllegalArgumentException("Can only get nex entity task for HC detector"));
                }
                break;
            case PUSH_BACK_ENTITY:
                logger.debug("Received PUSH_BACK_ENTITY action from task {}", adTask.getTaskId());
                if (detector.isMultientityDetector() && adTask.isEntityTask()) {
                    adTaskManager.removeRunningEntity(detectorId, adTask.getEntity());
                    if (adTaskManager.hcDetectorDone(detectorId)) {
                        adTaskManager.setHCDetectorTaskDone(adTask, ADTaskState.FINISHED, null, listener);
                    } else {
                        adTaskManager.runBatchResultActionForEntity(adTask, listener);
                    }
                } else {
                    listener.onFailure(new IllegalArgumentException("Can only push back entity task"));
                }
                break;
            case CANCEL:
                logger.debug("Received CANCEL action from task {}", adTask.getTaskId());
                if (detector.isMultientityDetector()) {
                    adTaskManager.clearPendingEntities(detectorId);
                    adTaskManager.removeRunningEntity(detectorId, adTask.getEntity());
                    if (adTaskManager.hcDetectorDone(detectorId) || !adTask.isEntityTask()) {
                        adTaskManager.setHCDetectorTaskDone(adTask, ADTaskState.STOPPED, adTask.getError(), listener);
                    }
                    listener.onResponse(new AnomalyDetectorJobResponse(adTask.getTaskId(), 0, 0, 0, RestStatus.OK));
                } else {
                    listener.onFailure(new IllegalArgumentException("Only support CANCEL action for HC detector"));
                }
                break;
            case CLEAN_RUNNING_ENTITY:
                logger.debug("Received CLEAN_RUNNING_ENTITY action from task {}", adTask.getTaskId());
                List<String> staleRunningEntities = request.getStaleRunningEntities();
                for (String entity : staleRunningEntities) {
                    // TODO: sleep for some time before run next entity?
                    adTaskManager.removeStaleRunningEntity(adTask, entity, listener);
                }
                listener.onResponse(new AnomalyDetectorJobResponse(adTask.getTaskId(), 0, 0, 0, RestStatus.OK));
                break;
            default:
                listener.onFailure(new ElasticsearchStatusException("Unsupported AD task action " + adTaskAction, RestStatus.BAD_REQUEST));
                break;
        }
    }
}
