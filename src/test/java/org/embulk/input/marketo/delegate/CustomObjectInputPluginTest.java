package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.EmbulkTestRuntime;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.input.marketo.model.MarketoField;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.input.marketo.rest.RecordPagingIterable;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

public class CustomObjectInputPluginTest
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Rule
    public EmbulkTestRuntime embulkTestRuntime = new EmbulkTestRuntime();

    private CustomObjectInputPlugin customObjectInputPlugin;

    private ConfigSource configSource;

    private MarketoRestClient mockMarketoRestClient;

    @Before
    public void setUp() throws Exception
    {
        customObjectInputPlugin = Mockito.spy(new CustomObjectInputPlugin());
        ConfigLoader configLoader = embulkTestRuntime.getInjector().getInstance(ConfigLoader.class);
        configSource = configLoader.fromYaml(this.getClass().getResourceAsStream("/config/custom_object_config.yaml"));
        mockMarketoRestClient = Mockito.mock(MarketoRestClient.class);
        Mockito.doReturn(mockMarketoRestClient).when(customObjectInputPlugin).createMarketoRestClient(Mockito.any(CustomObjectInputPlugin.PluginTask.class));
    }

    @Test
    public void testValidPluginTask()
    {
        CustomObjectInputPlugin.PluginTask pluginTask = configSource.loadConfig(CustomObjectInputPlugin.PluginTask.class);
        // should not cause exception
        customObjectInputPlugin.validateInputTask(pluginTask);
    }

    @Test
    public void testCustomObjectFilterTypeError()
    {
        CustomObjectInputPlugin.PluginTask pluginTask = configSource
                .set("custom_object_filter_type", "")
                .loadConfig(CustomObjectInputPlugin.PluginTask.class);
        Assert.assertThrows(ConfigException.class, () -> customObjectInputPlugin.validateInputTask(pluginTask));
    }

    @Test
    public void testCustomObjectAPINameError()
    {
        CustomObjectInputPlugin.PluginTask pluginTask = configSource
                .set("custom_object_api_name", "")
                .loadConfig(CustomObjectInputPlugin.PluginTask.class);
        Assert.assertThrows(ConfigException.class, () -> customObjectInputPlugin.validateInputTask(pluginTask));
    }

    @Test
    public void testFromValueGreaterThanToValueError()
    {
        CustomObjectInputPlugin.PluginTask pluginTask = configSource
                .set("custom_object_filter_from_value", "10")
                .set("custom_object_filter_to_value", "5").loadConfig(CustomObjectInputPlugin.PluginTask.class);
        Assert.assertThrows(ConfigException.class, () -> customObjectInputPlugin.validateInputTask(pluginTask));
    }

    @Test
    public void testEmptyStringFilterValues()
    {
        CustomObjectInputPlugin.PluginTask pluginTask = configSource
                .set("custom_object_filter_values", "").loadConfig(CustomObjectInputPlugin.PluginTask.class);

        Assert.assertThrows(ConfigException.class, () -> customObjectInputPlugin.validateInputTask(pluginTask));
    }

    @Test
    public void testAllEmptyStringFilterValues()
    {
        CustomObjectInputPlugin.PluginTask pluginTask = configSource
                .set("custom_object_filter_values", ",, , ").loadConfig(CustomObjectInputPlugin.PluginTask.class);

        Assert.assertThrows(ConfigException.class, () -> customObjectInputPlugin.validateInputTask(pluginTask));
    }

    @Test
    public void testRunStringFilterValues() throws IOException
    {
        RecordPagingIterable<ObjectNode> mockRecordPagingIterable = mockAndGetResponse();
        Mockito.when(mockMarketoRestClient.getCustomObject(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString())).thenReturn(mockRecordPagingIterable);

        CustomObjectInputPlugin.PluginTask task = configSource.set("custom_object_filter_values", "value1,value2,value3,value4").loadConfig(CustomObjectInputPlugin.PluginTask.class);
        ServiceResponseMapper<? extends ValueLocator> mapper = customObjectInputPlugin.buildServiceResponseMapper(task);
        RecordImporter recordImporter = mapper.createRecordImporter();
        PageBuilder mockPageBuilder = Mockito.mock(PageBuilder.class);
        customObjectInputPlugin.ingestServiceData(task, recordImporter, 1, mockPageBuilder);
        Mockito.verify(mockMarketoRestClient, Mockito.times(1)).getCustomObject(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

        verifyAfterRun(mapper, mockPageBuilder);
    }

    @Test
    public void testRunWithFilterRange() throws IOException
    {
        RecordPagingIterable<ObjectNode> mockRecordPagingIterable = mockAndGetResponse();
        Mockito.when(mockMarketoRestClient.getCustomObject(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString())).thenReturn(mockRecordPagingIterable);

        CustomObjectInputPlugin.PluginTask task = configSource.loadConfig(CustomObjectInputPlugin.PluginTask.class);
        ServiceResponseMapper<? extends ValueLocator> mapper = customObjectInputPlugin.buildServiceResponseMapper(task);
        RecordImporter recordImporter = mapper.createRecordImporter();
        PageBuilder mockPageBuilder = Mockito.mock(PageBuilder.class);
        customObjectInputPlugin.ingestServiceData(task, recordImporter, 1, mockPageBuilder);
        Mockito.verify(mockMarketoRestClient, Mockito.times(1)).getCustomObject(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());

        verifyAfterRun(mapper, mockPageBuilder);
    }

    @Test
    public void testRunFromOnlyFilter() throws IOException
    {
        RecordPagingIterable<ObjectNode> mockRecordPagingIterable = mockAndGetResponse();
        Mockito.when(mockMarketoRestClient.getCustomObject(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyInt(), Mockito.isNull())).thenReturn(mockRecordPagingIterable);

        CustomObjectInputPlugin.PluginTask task = configSource.remove("custom_object_filter_to_value").loadConfig(CustomObjectInputPlugin.PluginTask.class);
        ServiceResponseMapper<? extends ValueLocator> mapper = customObjectInputPlugin.buildServiceResponseMapper(task);
        RecordImporter recordImporter = mapper.createRecordImporter();
        PageBuilder mockPageBuilder = Mockito.mock(PageBuilder.class);
        customObjectInputPlugin.ingestServiceData(task, recordImporter, 1, mockPageBuilder);
        Mockito.verify(mockMarketoRestClient, Mockito.times(1)).getCustomObject(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyInt(), Mockito.isNull());

        verifyAfterRun(mapper, mockPageBuilder);
    }

    private void verifyAfterRun(ServiceResponseMapper<? extends ValueLocator> mapper, PageBuilder mockPageBuilder)
    {
        Mockito.verify(mockMarketoRestClient, Mockito.times(1)).describeCustomObject(Mockito.anyString());
        Schema schema = mapper.getEmbulkSchema();
        ArgumentCaptor<Long> longArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        Mockito.verify(mockPageBuilder, Mockito.times(3)).setLong(Mockito.eq(schema.lookupColumn("mk_id")), longArgumentCaptor.capture());
        List<Long> allValues = longArgumentCaptor.getAllValues();
        assertArrayEquals(new Long[]{1L, 2L, 3L}, allValues.toArray());
    }

    private RecordPagingIterable<ObjectNode> mockAndGetResponse() throws IOException
    {
        RecordPagingIterable<ObjectNode> mockRecordPagingIterable = Mockito.mock(RecordPagingIterable.class);
        JavaType javaType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, ObjectNode.class);
        List<ObjectNode> objectNodeList = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/custom_object_response_full.json"), javaType);
        JavaType marketoFieldsType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, MarketoField.class);
        List<MarketoField> marketoFields = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/custom_object_describe_marketo_fields_full.json"), marketoFieldsType);
        Mockito.when(mockRecordPagingIterable.iterator()).thenReturn(objectNodeList.iterator());
        Mockito.when(mockMarketoRestClient.describeCustomObject(Mockito.anyString())).thenReturn(marketoFields);
        return mockRecordPagingIterable;
    }
}
