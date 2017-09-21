package org.embulk.input.marketo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.embulk.base.restclient.DispatchingRestClientInputPluginDelegate;
import org.embulk.base.restclient.RestClientInputPluginDelegate;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.input.marketo.delegate.ActivityBulkExtractInputPlugin;
import org.embulk.input.marketo.delegate.CampaignInputPlugin;
import org.embulk.input.marketo.delegate.LeadBulkExtractInputPlugin;
import org.embulk.input.marketo.delegate.LeadWithListInputPlugin;
import org.embulk.input.marketo.delegate.LeadWithProgramInputPlugin;
import org.embulk.input.marketo.rest.MarketoRestClient;

public class MarketoInputPluginDelegate
        extends DispatchingRestClientInputPluginDelegate<MarketoInputPluginDelegate.PluginTask>
{
    public interface PluginTask
            extends LeadWithListInputPlugin.PluginTask,
            LeadBulkExtractInputPlugin.PluginTask,
            LeadWithProgramInputPlugin.PluginTask,
            ActivityBulkExtractInputPlugin.PluginTask,
            CampaignInputPlugin.PluginTask, MarketoRestClient.PluginTask
    {
        @Config("target")
        Target getTarget();

        @Config("maximum_retries")
        @ConfigDefault("3")
        Integer getMaximumRetries();

        @Config("initial_retry_interval_milis")
        @ConfigDefault("20000")
        Integer getInitialRetryIntervalMilis();

        @Config("maximum_retries_interval_milis")
        @ConfigDefault("120000")
        Integer getMaximumRetriesIntervalMilis();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RestClientInputPluginDelegate dispatchPerTask(PluginTask task)
    {
        Target target = task.getTarget();
        return target.getRestClientInputPluginDelegate();
    }

    public enum Target
    {
        LEAD(new LeadBulkExtractInputPlugin()),
        ACTIVITY(new ActivityBulkExtractInputPlugin()),
        CAMPAIGN(new CampaignInputPlugin()),
        ALL_LEAD_WITH_LIST_ID(new LeadWithListInputPlugin()),
        ALL_LEAD_WITH_PROGRAM_ID(new LeadWithProgramInputPlugin());

        private RestClientInputPluginDelegate restClientInputPluginDelegate;

        Target(RestClientInputPluginDelegate restClientInputPluginDelegate)
        {
            this.restClientInputPluginDelegate = restClientInputPluginDelegate;
        }

        @JsonIgnore
        public RestClientInputPluginDelegate getRestClientInputPluginDelegate()
        {
            return restClientInputPluginDelegate;
        }

        @JsonCreator
        public static Target of(String value)
        {
            return Target.valueOf(value.toUpperCase());
        }
    }
}
