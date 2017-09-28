package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoServiceImpl;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;

/**
 * Created by tai.khuu on 9/18/17.
 */
public class LeadBulkExtractInputPlugin extends MarketoBaseBulkExtractInputPlugin<LeadBulkExtractInputPlugin.PluginTask>
{
    private static final Logger LOGGER = Exec.getLogger(LeadBulkExtractInputPlugin.class);

    public interface PluginTask extends MarketoBaseBulkExtractInputPlugin.PluginTask
    {
    }

    public LeadBulkExtractInputPlugin()
    {
        super("updatedAt", null);
    }

    @Override
    protected InputStream getExtractedStream(PluginTask task, Schema schema)
    {
        try (MarketoRestClient marketoRestClient = createMarketoRestClient(task)) {
            MarketoService marketoService = new MarketoServiceImpl(marketoRestClient);
            List<String> fieldNames = task.getExtractedFields();
            Date fromDate = task.getFromDate();
            File file = marketoService.extractLead(fromDate, task.getToDate().get(), fieldNames, task.getPollingIntervalSecond(), task.getBulkJobTimeoutSecond());
            return new FileInputStream(file);
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
            List<MarketoField> columns = marketoService.describeLead();
            task.setExtractedFields(MarketoUtils.getFieldNameFromMarketoFields(columns));
            return MarketoUtils.buildDynamicResponseMapper(task.getSchemaColumnPrefix(), columns);
        }
    }
}
