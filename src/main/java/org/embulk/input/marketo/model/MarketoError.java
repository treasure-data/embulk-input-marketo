package org.embulk.input.marketo.model;

/**
 * Created by tai.khuu on 8/25/17.
 */
public class MarketoError
{
    private String code;

    private String message;

    public String getCode()
    {
        return code;
    }

    public void setCode(String code)
    {
        this.code = code;
    }

    public String getMessage()
    {
        return message;
    }

    public void setMessage(String message)
    {
        this.message = message;
    }

    @Override
    public String toString()
    {
        return "MarketoError{" +
                "code='" + code + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}
