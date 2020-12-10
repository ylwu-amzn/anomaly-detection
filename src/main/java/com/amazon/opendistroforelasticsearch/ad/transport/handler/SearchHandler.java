package com.amazon.opendistroforelasticsearch.ad.transport.handler;

import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES;
import static com.amazon.opendistroforelasticsearch.ad.util.ParseUtils.addUserBackendRolesFilter;
import static com.amazon.opendistroforelasticsearch.ad.util.ParseUtils.getUserContext;

public class SearchHandler {
    private final Logger logger = LogManager.getLogger(SearchHandler.class);
    private final Client client;
    private volatile Boolean filterEnabled;

    public SearchHandler(Settings settings, ClusterService clusterService, Client client) {

        this.client = client;
        filterEnabled = AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES, it -> filterEnabled = it);
    }

    public void search(SearchRequest request, ActionListener<SearchResponse> listener) {
        User user = getUserContext(client);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            validateRole(request, user, listener);
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    private void validateRole(SearchRequest request, User user, ActionListener<SearchResponse> listener) {
        if (user == null) {
            // Auth Header is empty when 1. Security is disabled. 2. When user is super-admin
            // Proceed with search
            searchDocs(request, listener);
        } else if (!filterEnabled) {
            // Security is enabled and filter is disabled
            // Proceed with search as user is already authenticated to hit this API.
            searchDocs(request, listener);
        } else {
            // Security is enabled and filter is enabled
            try {
                addUserBackendRolesFilter(user, request.source());
                searchDocs(request, listener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }

    private void searchDocs(SearchRequest request, ActionListener<SearchResponse> listener) {
        client.search(request, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                listener.onResponse(searchResponse);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

}
