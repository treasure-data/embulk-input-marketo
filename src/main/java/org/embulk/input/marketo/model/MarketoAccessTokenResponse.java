package org.embulk.input.marketo.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by tai.khuu on 8/25/17.
 */
public class MarketoAccessTokenResponse
{
    @JsonProperty("error")
    private String error;

    @JsonProperty("error_description")
    private String errorDescription;

    @JsonProperty("access_token")
    private String accessToken;

    @JsonProperty("token_type")
    private String tokenType;

    @JsonProperty("expires_in")
    private Integer expiresIn;

    @JsonProperty("scope")
    private String scope;

    public String getError()
    {
        return error;
    }

    public void setError(String error)
    {
        this.error = error;
    }

    public String getErrorDescription()
    {
        return errorDescription;
    }

    public void setErrorDescription(String errorDescription)
    {
        this.errorDescription = errorDescription;
    }

    public String getAccessToken()
    {
        return accessToken;
    }

    public void setAccessToken(String accessToken)
    {
        this.accessToken = accessToken;
    }

    public String getTokenType()
    {
        return tokenType;
    }

    public void setTokenType(String tokenType)
    {
        this.tokenType = tokenType;
    }

    public Integer getExpiresIn()
    {
        return expiresIn;
    }

    public void setExpiresIn(Integer expiresIn)
    {
        this.expiresIn = expiresIn;
    }

    public String getScope()
    {
        return scope;
    }

    public void setScope(String scope)
    {
        this.scope = scope;
    }

    public boolean hasError()
    {
        return error != null;
    }
}
