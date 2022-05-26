package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.JavaType;
import org.embulk.EmbulkTestRuntime;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.input.marketo.MarketoService;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.spi.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.embulk.input.marketo.MarketoUtilsTest.CONFIG_MAPPER;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by tai.khuu on 5/24/18.
 */
public class LeadServiceResponseMapperBuilderTest
{
    private LeadServiceResponseMapperBuilder<LeadServiceResponseMapperBuilder.PluginTask> leadServiceResponseMapperBuilder;

    @Rule
    public EmbulkTestRuntime embulkTestRuntime = new EmbulkTestRuntime();

    private LeadServiceResponseMapperBuilder.PluginTask pluginTask;

    private ConfigSource configSource;

    private MarketoService marketoService;

    @Before
    public void setUp() throws Exception
    {
        ConfigLoader configLoader = embulkTestRuntime.getExec().getInjector().getInstance(ConfigLoader.class);
        configSource = configLoader.fromYamlString("--- \n" +
                "account_id: 389-FLQ-873\n" +
                "client_id: 87bdd058-12ff-49b8-8d29-c0518669e59a\n" +
                "client_secret: 2uauyk43zzwjlUZwboZVxQ3SMTHe8ILY\n" +
                "included_fields: \n" +
                "  - company\n" +
                "  - site\n" +
                "  - billingstreet\n" +
                "  - billingcity\n" +
                "  - billingstate\n" +
                "  - billingcountry\n" +
                "  - billingpostalCode\n" +
                "  - website\n" +
                "  - mainphone\n" +
                "  - annualrevenue\n" +
                "  - numberofemployees\n" +
                "  - industry\n" +
                "  - siccode\n" +
                "  - mktoCompanyNotes\n" +
                "  - externalcompanyId\n" +
                "  - id\n" +
                "target: all_lead_with_list_id\n");
        pluginTask = CONFIG_MAPPER.map(configSource, LeadServiceResponseMapperBuilder.PluginTask.class);
        marketoService = mock(MarketoService.class);
        JavaType marketoFieldsType = MarketoUtils.OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, MarketoField.class);
        List<MarketoField> marketoFields = MarketoUtils.OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/lead_describe_marketo_fields_full.json"), marketoFieldsType);
        when(marketoService.describeLead()).thenReturn(marketoFields);
    }

    @Test
    public void buildServiceResponseMapper()
    {
        leadServiceResponseMapperBuilder = new LeadServiceResponseMapperBuilder<>(pluginTask, marketoService);
        ServiceResponseMapper<? extends ValueLocator> serviceResponseMapper = leadServiceResponseMapperBuilder.buildServiceResponseMapper(pluginTask);
        Assert.assertFalse(pluginTask.getExtractedFields().isEmpty());
        Assert.assertEquals(16, pluginTask.getExtractedFields().size());
        Schema embulkSchema = serviceResponseMapper.getEmbulkSchema();
        Assert.assertEquals(16, embulkSchema.getColumns().size());
        Assert.assertEquals("mk_billingStreet", embulkSchema.getColumns().get(2).getName());
        Assert.assertEquals("mk_company", embulkSchema.getColumn(0).getName());
        Assert.assertEquals("mk_billingStreet", embulkSchema.getColumn(2).getName());
        Assert.assertEquals("mk_id", embulkSchema.getColumn(15).getName());
    }

    @Test
    public void getLeadColumnsIncludedEmpty() throws IOException
    {
        configSource = configSource.set("included_fields", MarketoUtils.OBJECT_MAPPER.readTree("[]"));
        pluginTask = CONFIG_MAPPER.map(configSource, LeadServiceResponseMapperBuilder.PluginTask.class);
        leadServiceResponseMapperBuilder = new LeadServiceResponseMapperBuilder<>(pluginTask, marketoService);
        List<MarketoField> leadColumns = leadServiceResponseMapperBuilder.getLeadColumns();
        Assert.assertEquals(129, leadColumns.size());
    }

    @Test
    public void getLeadColumnsIncluded1() throws IOException
    {
        configSource = configSource.set("included_fields", MarketoUtils.OBJECT_MAPPER.readTree("[\"company\",\"incorrect_value\"]"));
        pluginTask = CONFIG_MAPPER.map(configSource, LeadServiceResponseMapperBuilder.PluginTask.class);
        leadServiceResponseMapperBuilder = new LeadServiceResponseMapperBuilder<>(pluginTask, marketoService);
        List<MarketoField> leadColumns = leadServiceResponseMapperBuilder.getLeadColumns();
        Assert.assertEquals(1, leadColumns.size());
        Assert.assertEquals("company", leadColumns.get(0).getName());
    }

    @Test
    public void getLeadColumnsIncluded2() throws IOException
    {
        configSource = configSource.set("included_fields", MarketoUtils.OBJECT_MAPPER.readTree("[\"company\",\"incorrect_value\"]"));
        pluginTask = CONFIG_MAPPER.map(configSource, LeadServiceResponseMapperBuilder.PluginTask.class);
        marketoService = mock(MarketoService.class);
        leadServiceResponseMapperBuilder = new LeadServiceResponseMapperBuilder<>(pluginTask, marketoService);
        when(marketoService.describeLead()).thenReturn(new ArrayList<>());
        List<MarketoField> leadColumns = leadServiceResponseMapperBuilder.getLeadColumns();
        Assert.assertTrue(leadColumns.isEmpty());
    }
}
