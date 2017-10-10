package org.embulk.input.marketo.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by tai.khuu on 8/25/17.
 */
public class MarketoResponse<T>
{
    private String requestId;

    private boolean success;

    private String nextPageToken;

    private boolean moreResult;

    private List<MarketoError> errors;

    private List<T> result = new ArrayList<>();

    public String getRequestId()
    {
        return requestId;
    }

    public void setRequestId(String requestId)
    {
        this.requestId = requestId;
    }

    public boolean isSuccess()
    {
        return success;
    }

    public void setSuccess(boolean success)
    {
        this.success = success;
    }

    public List<MarketoError> getErrors()
    {
        return errors;
    }

    public void setErrors(List<MarketoError> errors)
    {
        this.errors = errors;
    }

    public List<T> getResult()
    {
        return result;
    }

    public void setResult(List<T> result)
    {
        this.result = result;
    }

    public String getNextPageToken()
    {
        return nextPageToken;
    }

    public void setNextPageToken(String nextPageToken)
    {
        this.nextPageToken = nextPageToken;
    }

    public boolean isMoreResult()
    {
        return moreResult;
    }

    public void setMoreResult(boolean moreResult)
    {
        this.moreResult = moreResult;
    }
}
