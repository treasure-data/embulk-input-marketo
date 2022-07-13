package org.embulk.input.marketo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;

import org.embulk.base.restclient.DispatchingRestClientInputPluginDelegate;
import org.embulk.base.restclient.RestClientInputPluginDelegate;
import org.embulk.config.ConfigException;
import org.embulk.input.marketo.delegate.ActivityBulkExtractInputPlugin;
import org.embulk.input.marketo.delegate.ActivityTypeInputPlugin;
import org.embulk.input.marketo.delegate.CampaignInputPlugin;
import org.embulk.input.marketo.delegate.CustomObjectInputPlugin;
import org.embulk.input.marketo.delegate.LeadBulkExtractInputPlugin;
import org.embulk.input.marketo.delegate.LeadWithListInputPlugin;
import org.embulk.input.marketo.delegate.LeadWithProgramInputPlugin;
import org.embulk.input.marketo.delegate.ListInputPlugin;
import org.embulk.input.marketo.delegate.ProgramInputPlugin;
import org.embulk.input.marketo.delegate.ProgramMembersBulkExtractInputPlugin;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;

import java.util.Date;
import java.util.Optional;

public class MarketoInputPluginDelegate
        extends DispatchingRestClientInputPluginDelegate<MarketoInputPluginDelegate.PluginTask>
{
    public interface PluginTask
            extends LeadWithListInputPlugin.PluginTask,
            LeadBulkExtractInputPlugin.PluginTask,
            LeadWithProgramInputPlugin.PluginTask,
            ActivityBulkExtractInputPlugin.PluginTask,
            CampaignInputPlugin.PluginTask,
            ProgramInputPlugin.PluginTask,
            MarketoRestClient.PluginTask,
            CustomObjectInputPlugin.PluginTask,
            ProgramMembersBulkExtractInputPlugin.PluginTask,
            ListInputPlugin.PluginTask,
            ActivityTypeInputPlugin.PluginTask
    {
        @Config("target")
        Target getTarget();

        //We don't need to let the internal plugin know that it being dispatched and force it to set require field optional
        //We will hide the real from_date, and set it when validating task
        @Config("hidden_from_date")
        @ConfigDefault("\"1970-01-01\"")
        @Override
        Date getFromDate();

        void setFromDate(Date fromDate);

        @Config("from_date")
        @ConfigDefault("null")
        Optional<Date> getWrappedFromDate();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected RestClientInputPluginDelegate dispatchPerTask(PluginTask task)
    {
        Target target = task.getTarget();
        switch (target) {
            case LEAD:
            case ACTIVITY:
                if (!task.getWrappedFromDate().isPresent()) {
                    throw new ConfigException("From date is required for target LEAD or ACTIVITY");
                }
                Date date = task.getWrappedFromDate().get();
                task.setFromDate(date);
                break;
        }
        return target.getRestClientInputPluginDelegate();
    }

    public enum Target
    {
        LEAD(new LeadBulkExtractInputPlugin()),
        ACTIVITY(new ActivityBulkExtractInputPlugin()),
        CAMPAIGN(new CampaignInputPlugin()),
        ALL_LEAD_WITH_LIST_ID(new LeadWithListInputPlugin()),
        ALL_LEAD_WITH_PROGRAM_ID(new LeadWithProgramInputPlugin()),
        PROGRAM(new ProgramInputPlugin()),
        CUSTOM_OBJECT(new CustomObjectInputPlugin()),
        PROGRAM_MEMBERS(new ProgramMembersBulkExtractInputPlugin()),
        LIST(new ListInputPlugin()),
        ACTIVITY_TYPE(new ActivityTypeInputPlugin());

        private final RestClientInputPluginDelegate restClientInputPluginDelegate;

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
