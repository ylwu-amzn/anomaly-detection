package com.amazon.opendistroforelasticsearch.ad.transport;

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class ForwardADTaskRequest extends ActionRequest {
    private AnomalyDetector detector;
    private User user;
    //TODO: add user

    public ForwardADTaskRequest(AnomalyDetector detector, User user) {
        this.detector = detector;
    }

    public ForwardADTaskRequest(StreamInput in) throws IOException {
        super(in);
        this.detector = new AnomalyDetector(in);
        if (in.readBoolean()) {
            this.user = new User(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        detector.writeTo(out);
        if (user != null) {
            out.writeBoolean(true);
            user.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (detector == null) {
            validationException =  addValidationError("Detector is missing", validationException);
        }
        return validationException;
    }

    public AnomalyDetector getDetector() {
        return detector;
    }

    public User getUser() {
        return user;
    }
}
