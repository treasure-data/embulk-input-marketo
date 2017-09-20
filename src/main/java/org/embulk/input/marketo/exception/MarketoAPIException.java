package org.embulk.input.marketo.exception;

import org.embulk.input.marketo.model.MarketoError;

import java.util.List;

/**
 * Exception class for all API Exception
 * Created by tai.khuu on 9/5/17.
 */
public class MarketoAPIException extends Exception
{
    private final List<MarketoError> marketoErrors;
    public MarketoAPIException(List<MarketoError> marketoErrors)
    {
        this.marketoErrors = marketoErrors;
    }

    public List<MarketoError> getMarketoErrors()
    {
        return marketoErrors;
    }

    @Override
    public String getMessage()
    {
        MarketoError error = getMarketoErrors().get(0);
        return "Marketo API Error, code: " + error.getCode() + ", message: " + error.getMessage();
    }
}
