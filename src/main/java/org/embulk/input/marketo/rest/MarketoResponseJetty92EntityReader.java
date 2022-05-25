package org.embulk.input.marketo.rest;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.CharStreams;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.InputStreamResponseListener;
import org.embulk.input.marketo.exception.MarketoAPIException;
import org.embulk.input.marketo.model.MarketoResponse;
import org.embulk.spi.DataException;
import org.embulk.util.retryhelper.jetty92.Jetty92ResponseReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Created by tai.khuu on 9/1/17.
 */
public class MarketoResponseJetty92EntityReader<T> implements Jetty92ResponseReader<MarketoResponse<T>>
{
    private InputStreamResponseListener listener;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final Logger LOGGER = LoggerFactory.getLogger(MarketoResponseJetty92EntityReader.class);
    private final Long timeout;

    private final JavaType javaType;

    public static String jsonResponseInvalid = "Exception when parse json content";

    public MarketoResponseJetty92EntityReader(long timeout)
    {
        this.timeout = timeout;
        javaType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(MarketoResponse.class, MarketoResponse.class, ObjectNode.class);
    }

    public MarketoResponseJetty92EntityReader(long timeout, Class<T> resultClass)
    {
        this.listener = new InputStreamResponseListener();
        this.timeout = timeout;
        this.javaType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(MarketoResponse.class, MarketoResponse.class, resultClass);
    }

    @Override
    public Response.Listener getListener()
    {
        this.listener = new InputStreamResponseListener();
        return this.listener;
    }

    @Override
    public Response getResponse() throws Exception
    {
        return this.listener.get(timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public MarketoResponse<T> readResponseContent() throws Exception
    {
        String response = readResponseContentInString();
        try {
            MarketoResponse<T> marketoResponse = OBJECT_MAPPER.readValue(response, javaType);
            if (!marketoResponse.isSuccess()) {
                throw new MarketoAPIException(marketoResponse.getErrors());
            }
            return marketoResponse;
        }
        catch (IOException ex) {
            LOGGER.error("Can't parse json content", ex);
            throw new DataException(jsonResponseInvalid);
        }
    }

    @Override
    public String readResponseContentInString() throws Exception
    {
        InputStream inputStream = this.listener.getInputStream();
        try (InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
            return CharStreams.toString(inputStreamReader);
        }
    }
}
