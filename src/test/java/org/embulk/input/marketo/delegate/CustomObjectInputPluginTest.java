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

import java.io.IOException;
import java.util.List;

import static org.embulk.input.marketo.MarketoUtilsTest.CONFIG_MAPPER;
import static org.embulk.input.marketo.delegate.CustomObjectInputPlugin.PluginTask;
import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
        customObjectInputPlugin = spy(new CustomObjectInputPlugin());
        ConfigLoader configLoader = embulkTestRuntime.getInjector().getInstance(ConfigLoader.class);
        configSource = configLoader.fromYaml(this.getClass().getResourceAsStream("/config/custom_object_config.yaml"));
        mockMarketoRestClient = mock(MarketoRestClient.class);
        doReturn(mockMarketoRestClient).when(customObjectInputPlugin).createMarketoRestClient(any(PluginTask.class));
    }

    @Test
    public void testValidPluginTask()
    {
        PluginTask pluginTask = mapTask(configSource);
        // should not cause exception
        customObjectInputPlugin.validateInputTask(pluginTask);
    }

    @Test
    public void testCustomObjectFilterTypeError()
    {
        PluginTask pluginTask = mapTask(configSource.set("custom_object_filter_type", ""));
        Assert.assertThrows(ConfigException.class, () -> customObjectInputPlugin.validateInputTask(pluginTask));
    }

    @Test
    public void testCustomObjectAPINameError()
    {
        PluginTask pluginTask = mapTask(configSource.set("custom_object_api_name", ""));
        Assert.assertThrows(ConfigException.class, () -> customObjectInputPlugin.validateInputTask(pluginTask));
    }

    @Test
    public void testFromValueGreaterThanToValueError()
    {
        PluginTask pluginTask = mapTask(configSource
                .set("custom_object_filter_from_value", "10")
                .set("custom_object_filter_to_value", "5"));
        Assert.assertThrows(ConfigException.class, () -> customObjectInputPlugin.validateInputTask(pluginTask));
    }

    @Test
    public void testEmptyStringFilterValues()
    {
        PluginTask pluginTask = mapTask(configSource.set("custom_object_filter_values", ""));
        Assert.assertThrows(ConfigException.class, () -> customObjectInputPlugin.validateInputTask(pluginTask));
    }

    @Test
    public void testAllEmptyStringFilterValues()
    {
        PluginTask pluginTask = mapTask(configSource.set("custom_object_filter_values", ",, , "));
        Assert.assertThrows(ConfigException.class, () -> customObjectInputPlugin.validateInputTask(pluginTask));
    }

    @Test
    public void testRunStringFilterValues() throws IOException
    {
        RecordPagingIterable<ObjectNode> mockRecordPagingIterable = mockAndGetResponse();
        when(mockMarketoRestClient.getCustomObject(anyString(), anyString(), anyString(), anyString())).thenReturn(mockRecordPagingIterable);

        PluginTask task = mapTask(configSource.set("custom_object_filter_values", "value1,value2,value3,value4"));
        ServiceResponseMapper<? extends ValueLocator> mapper = customObjectInputPlugin.buildServiceResponseMapper(task);
        RecordImporter recordImporter = mapper.createRecordImporter();
        PageBuilder mockPageBuilder = mock(PageBuilder.class);
        customObjectInputPlugin.ingestServiceData(task, recordImporter, 1, mockPageBuilder);
        verify(mockMarketoRestClient, times(1)).getCustomObject(anyString(), anyString(), anyString(), anyString());

        verifyAfterRun(mapper, mockPageBuilder);
    }

    @Test
    public void testRunWithFilterRange() throws IOException
    {
        RecordPagingIterable<ObjectNode> mockRecordPagingIterable = mockAndGetResponse();
        when(mockMarketoRestClient.getCustomObject(anyString(), anyString(), anyString(), anyString())).thenReturn(mockRecordPagingIterable);

        PluginTask task = mapTask(configSource);
        ServiceResponseMapper<? extends ValueLocator> mapper = customObjectInputPlugin.buildServiceResponseMapper(task);
        RecordImporter recordImporter = mapper.createRecordImporter();
        PageBuilder mockPageBuilder = mock(PageBuilder.class);
        customObjectInputPlugin.ingestServiceData(task, recordImporter, 1, mockPageBuilder);
        verify(mockMarketoRestClient, times(1)).getCustomObject(anyString(), anyString(), anyString(), anyString());

        verifyAfterRun(mapper, mockPageBuilder);
    }

    @Test
    public void testRunFromOnlyFilter() throws IOException
    {
        RecordPagingIterable<ObjectNode> mockRecordPagingIterable = mockAndGetResponse();
        when(mockMarketoRestClient.getCustomObject(anyString(), anyString(), anyString(), anyInt(), isNull())).thenReturn(mockRecordPagingIterable);

        PluginTask task = mapTask(configSource.remove("custom_object_filter_to_value"));
        ServiceResponseMapper<? extends ValueLocator> mapper = customObjectInputPlugin.buildServiceResponseMapper(task);
        RecordImporter recordImporter = mapper.createRecordImporter();
        PageBuilder mockPageBuilder = mock(PageBuilder.class);
        customObjectInputPlugin.ingestServiceData(task, recordImporter, 1, mockPageBuilder);
        verify(mockMarketoRestClient, times(1)).getCustomObject(anyString(), anyString(), anyString(), anyInt(), isNull());

        verifyAfterRun(mapper, mockPageBuilder);
    }

    private void verifyAfterRun(ServiceResponseMapper<? extends ValueLocator> mapper, PageBuilder mockPageBuilder)
    {
        verify(mockMarketoRestClient, times(1)).describeCustomObject(anyString());
        Schema schema = mapper.getEmbulkSchema();
        ArgumentCaptor<Long> longArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        verify(mockPageBuilder, times(3)).setLong(eq(schema.lookupColumn("mk_id")), longArgumentCaptor.capture());
        List<Long> allValues = longArgumentCaptor.getAllValues();
        assertArrayEquals(new Long[]{1L, 2L, 3L}, allValues.toArray());
    }

    private RecordPagingIterable<ObjectNode> mockAndGetResponse() throws IOException
    {
        RecordPagingIterable<ObjectNode> mockRecordPagingIterable = mock(RecordPagingIterable.class);
        JavaType javaType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, ObjectNode.class);
        List<ObjectNode> objectNodeList = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/custom_object_response_full.json"), javaType);
        JavaType marketoFieldsType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, MarketoField.class);
        List<MarketoField> marketoFields = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/custom_object_describe_marketo_fields_full.json"), marketoFieldsType);
        when(mockRecordPagingIterable.iterator()).thenReturn(objectNodeList.iterator());
        when(mockMarketoRestClient.describeCustomObject(anyString())).thenReturn(marketoFields);
        return mockRecordPagingIterable;
    }

    private PluginTask mapTask(ConfigSource configSource)
    {
        return CONFIG_MAPPER.map(configSource, PluginTask.class);
    }
}
