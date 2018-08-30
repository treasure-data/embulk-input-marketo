package org.embulk.input.marketo.delegate;

import com.google.common.base.Optional;

import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoServiceImpl;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.joda.time.DateTime;
import org.slf4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;

/**
 * Created by tai.khuu on 9/18/17.
 */
public class LeadBulkExtractInputPlugin extends MarketoBaseBulkExtractInputPlugin<LeadBulkExtractInputPlugin.PluginTask>
{
    private static final Logger LOGGER = Exec.getLogger(LeadBulkExtractInputPlugin.class);

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
    protected InputStream getExtractedStream(MarketoService service, PluginTask task, DateTime fromDate, DateTime toDate)
    {
        try {
            List<String> fieldNames = task.getExtractedFields();
            return new FileInputStream(service.extractLead(fromDate.toDate(), toDate.toDate(), fieldNames, task.getIncrementalColumn().orNull(), task.getPollingIntervalSecond(), task.getBulkJobTimeoutSecond()));
        }
        catch (FileNotFoundException e) {
            LOGGER.error("File not found", e);
            throw new DataException("Error when extract lead");
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
