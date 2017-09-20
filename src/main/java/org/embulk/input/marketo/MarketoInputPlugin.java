package org.embulk.input.marketo;

import org.embulk.base.restclient.RestClientInputPluginBase;

/**
 * Created by tai.khuu on 8/22/17.
 */
public class MarketoInputPlugin
        extends RestClientInputPluginBase<MarketoInputPluginDelegate.PluginTask>
{
    public MarketoInputPlugin()
    {
        super(MarketoInputPluginDelegate.PluginTask.class, new MarketoInputPluginDelegate());
    }
}
