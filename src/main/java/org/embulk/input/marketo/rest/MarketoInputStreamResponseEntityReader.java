package org.embulk.input.marketo.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.io.CharStreams;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.InputStreamResponseListener;
import org.eclipse.jetty.http.HttpHeader;
import org.embulk.input.marketo.exception.MarketoAPIException;
import org.embulk.input.marketo.model.MarketoResponse;
import org.embulk.util.retryhelper.jetty92.Jetty92ResponseReader;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

/**
 * Created by tai.khuu on 9/5/17.
 */
public class MarketoInputStreamResponseEntityReader implements Jetty92ResponseReader<InputStream>
{
    private static final ObjectReader OBJECT_READER = new ObjectMapper().readerFor(new TypeReference<MarketoResponse<ObjectNode>>(){ });

    private InputStreamResponseListener listener;

    private long timeout;

    public MarketoInputStreamResponseEntityReader(long timeout)
    {
        this.timeout = timeout;
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
    public InputStream readResponseContent() throws Exception
    {
        if (!getResponse().getHeaders().getField(HttpHeader.CONTENT_TYPE).getValue().equals("text/csv")) {
            String errorString = readResponseContentInString();

            MarketoResponse<ObjectNode> errorResponse = OBJECT_READER.readValue(errorString);
            if (!errorResponse.isSuccess()) {
                throw new MarketoAPIException(errorResponse.getErrors());
            }
        }
        return this.listener.getInputStream();
    }

    @Override
    public String readResponseContentInString() throws Exception
    {
        try (InputStreamReader inputStreamReader = new InputStreamReader(this.listener.getInputStream())) {
            return CharStreams.toString(inputStreamReader);
        }
    }
}
