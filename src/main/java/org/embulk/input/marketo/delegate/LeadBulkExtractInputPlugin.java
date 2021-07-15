package org.embulk.input.marketo.delegate;

import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoServiceImpl;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * Created by tai.khuu on 9/18/17.
 */
public class LeadBulkExtractInputPlugin extends MarketoBaseBulkExtractInputPlugin<LeadBulkExtractInputPlugin.PluginTask>
{
    private static final String UPDATED_AT = "updatedAt";

    public interface PluginTask extends MarketoBaseBulkExtractInputPlugin.PluginTask, LeadServiceResponseMapperBuilder.PluginTask
    {
        @Config("use_updated_at")
        @ConfigDefault("false")
        boolean getUseUpdatedAt();
    }

    @Override
    public void validateInputTask(PluginTask task)
    {
        if (task.getUseUpdatedAt()) {
            task.setIncrementalColumn(Optional.of(UPDATED_AT));
        }
        super.validateInputTask(task);
    }

    @Override
    protected InputStream getExtractedStream(MarketoService service, PluginTask task, OffsetDateTime fromDate, OffsetDateTime toDate)
    {
        try {
            List<String> fieldNames = task.getExtractedFields();
            return new FileInputStream(service.extractLead(Date.from(fromDate.toInstant()), Date.from(toDate.toInstant()),
                    fieldNames, task.getIncrementalColumn().orElse(null), task.getPollingIntervalSecond(), task.getBulkJobTimeoutSecond()));
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException("File not found", e);
        }
    }

    @Override
    public ServiceResponseMapper<? extends ValueLocator> buildServiceResponseMapper(PluginTask task)
    {
        try (MarketoRestClient marketoRestClient = createMarketoRestClient(task)) {
            MarketoService marketoService = new MarketoServiceImpl(marketoRestClient);
            LeadServiceResponseMapperBuilder<PluginTask> leadServiceResponseMapperBuilder = new LeadServiceResponseMapperBuilder<>(task, marketoService);
            return leadServiceResponseMapperBuilder.buildServiceResponseMapper(task);
        }
    }
}
