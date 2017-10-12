package org.embulk.input.marketo.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentProvider;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.embulk.input.marketo.exception.MarketoAPIException;
import org.embulk.input.marketo.model.MarketoAccessTokenResponse;
import org.embulk.input.marketo.model.MarketoError;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.util.retryhelper.jetty92.Jetty92ResponseReader;
import org.embulk.util.retryhelper.jetty92.Jetty92RetryHelper;
import org.embulk.util.retryhelper.jetty92.Jetty92SingleRequester;
import org.embulk.util.retryhelper.jetty92.StringJetty92ResponseEntityReader;
import org.slf4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

/**
 * Marketo base rest client that provide
 * Created by tai.khuu on 9/7/17.
 */
public class MarketoBaseRestClient implements AutoCloseable
{
    private static final Logger LOGGER = Exec.getLogger(MarketoBaseRestClient.class);

    private static final String APPLICATION_JSON = "application/json";

    private static final String AUTHORIZATION_HEADER = "Authorization";

    protected static final long READ_TIMEOUT_MILLIS = 30000;

    private String identityEndPoint;

    private String clientId;

    private String clientSecret;

    private String accessToken;

    private int marketoLimitIntervalMilis;

    private Jetty92RetryHelper retryHelper;

    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().configure(FAIL_ON_UNKNOWN_PROPERTIES, false).configure(ALLOW_UNQUOTED_CONTROL_CHARS, false);

    MarketoBaseRestClient(String identityEndPoint, String clientId, String clientSecret, int marketoLimitIntervalMilis, Jetty92RetryHelper retryHelper)
    {
        this.identityEndPoint = identityEndPoint;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.retryHelper = retryHelper;
        this.marketoLimitIntervalMilis = marketoLimitIntervalMilis;
    }

    private void renewAccessToken()
    {
        accessToken = requestAccessToken();
    }

    @VisibleForTesting
    public String getAccessToken()
    {
        if (accessToken == null) {
            synchronized (this) {
                if (accessToken == null) {
                    accessToken = requestAccessToken();
                }
            }
        }
        return accessToken;
    }

    private String requestAccessToken()
    {
        final Multimap<String, String> params = ArrayListMultimap.create();
        params.put("client_id", clientId);
        params.put("client_secret", clientSecret);
        params.put("grant_type", "client_credentials");
        String response = retryHelper.requestWithRetry(new StringJetty92ResponseEntityReader(READ_TIMEOUT_MILLIS), new Jetty92SingleRequester()
        {
            @Override
            public void requestOnce(HttpClient client, Response.Listener responseListener)
            {
                Request request = client.newRequest(identityEndPoint + MarketoRESTEndpoint.ACCESS_TOKEN.getEndpoint()).method(HttpMethod.GET);
                for (String key : params.keySet()) {
                    for (String value : params.get(key)) {
                        request.param(key, value);
                    }
                }
                request.send(responseListener);
            }

            @Override
            protected boolean isResponseStatusToRetry(Response response)
            {
                return response.getStatus() == 502;
            }
        });

        MarketoAccessTokenResponse accessTokenResponse;

        try {
            accessTokenResponse = OBJECT_MAPPER.readValue(response, MarketoAccessTokenResponse.class);
        }
        catch (IOException e) {
            LOGGER.error("Exception when parse access token response", e);
            throw new DataException("Can't parse access token response");
        }
        if (accessTokenResponse.hasError()) {
            throw new DataException(accessTokenResponse.getErrorDescription());
        }
        LOGGER.info("Acquired new access token");
        return accessTokenResponse.getAccessToken();
    }

    protected <T> T doGet(final String target, final Map<String, String> headers, final Multimap<String, String> params, Jetty92ResponseReader<T> responseReader)
    {
        return doRequest(target, HttpMethod.GET, headers, params, null, responseReader);
    }

    protected <T> T doPost(final String target, final Map<String, String> headers, final Multimap<String, String> params, final String content, Jetty92ResponseReader<T> responseReader)
    {
        StringContentProvider contentProvider = null;
        if (content != null) {
            contentProvider = new StringContentProvider(APPLICATION_JSON, content, StandardCharsets.UTF_8);
        }
        return doPost(target, headers, params, responseReader, contentProvider);
    }

    protected <T> T doPost(final String target, final Map<String, String> headers, final Multimap<String, String> params, Jetty92ResponseReader<T> responseReader, final ContentProvider content)
    {
        return doRequest(target, HttpMethod.POST, headers, params, content, responseReader);
    }

    protected <T> T doRequest(final String target, final HttpMethod method, final Map<String, String> headers, final Multimap<String, String> params, final ContentProvider contentProvider, Jetty92ResponseReader<T> responseReader)
    {
        return retryHelper.requestWithRetry(responseReader, new Jetty92SingleRequester()
        {
            @Override
            public void requestOnce(HttpClient client, Response.Listener responseListener)
            {
                Request request = client.newRequest(target).method(method);
                if (headers != null) {
                    for (String key : headers.keySet()) {
                        request.header(key, headers.get(key));
                    }
                }
                request.header(AUTHORIZATION_HEADER, "Bearer " + getAccessToken());
                if (params != null) {
                    for (String key : params.keySet()) {
                        for (String value : params.get(key)) {
                            request.param(key, value);
                        }
                    }
                }
                if (contentProvider != null) {
                    request.content(contentProvider, APPLICATION_JSON);
                }
                request.send(responseListener);
            }

            @Override
            protected boolean isResponseStatusToRetry(Response response)
            {
                //413 failed job
                //414 failed job
                //502 retry
                return response.getStatus() / 4 != 100;
            }

            @Override
            protected boolean isExceptionToRetry(Exception exception)
            {
                if (exception instanceof ExecutionException) {
                    this.toRetry((Exception) exception.getCause());
                }
                //Anything that is EOFException or cause by EOF exception
                if (exception instanceof EOFException || exception.getCause() instanceof EOFException) {
                    return true;
                }
                if (exception instanceof MarketoAPIException) {
                    //Retry Authenticate Exception
                    MarketoError error = ((MarketoAPIException) exception).getMarketoErrors().get(0);
                    String code = error.getCode();
                    switch (code) {
                        case "602":
                            LOGGER.info("Access token expired");
                            renewAccessToken();
                            return true;
                        case "606":
                            try {
                                Thread.sleep(marketoLimitIntervalMilis);
                            }
                            catch (InterruptedException e) {
                                LOGGER.error("Encounter exception when waiting for interval limit", e);
                                throw new DataException("Exception when wait for interval limit");
                            }
                            return true;
                        case "615":
                            return true;
                        default:
                            return false;
                    }
                }
                return exception instanceof TimeoutException || exception instanceof SocketTimeoutException || super.isExceptionToRetry(exception);
            }
        });
    }

    @Override
    public void close()
    {
        if (retryHelper != null) {
            retryHelper.close();
        }
    }
}
