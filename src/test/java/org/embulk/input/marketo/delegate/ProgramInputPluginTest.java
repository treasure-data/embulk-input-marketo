package org.embulk.input.marketo.delegate;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.EmbulkTestRuntime;
import org.embulk.base.restclient.ServiceResponseMapper;
import org.embulk.base.restclient.record.RecordImporter;
import org.embulk.base.restclient.record.ValueLocator;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.input.marketo.MarketoUtils;
import org.embulk.input.marketo.delegate.ProgramInputPlugin.PluginTask;
import org.embulk.input.marketo.delegate.ProgramInputPlugin.QueryBy;
import org.embulk.input.marketo.rest.MarketoRestClient;
import org.embulk.input.marketo.rest.RecordPagingIterable;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.Schema;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static org.embulk.input.marketo.MarketoUtilsTest.CONFIG_MAPPER;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ProgramInputPluginTest
{
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern(MarketoUtils.MARKETO_DATE_SIMPLE_DATE_FORMAT);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private final ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestRule chain = RuleChain.outerRule(runtime).around(thrown);

    private ConfigSource baseConfig;

    private ProgramInputPlugin mockPlugin;

    private MarketoRestClient mockRestClient;

    @Before
    public void setUp() throws Exception
    {
        mockPlugin = Mockito.spy(new ProgramInputPlugin());
        baseConfig = config();
        mockRestClient = Mockito.mock(MarketoRestClient.class);
        Mockito.doReturn(mockRestClient).when(mockPlugin).createMarketoRestClient(Mockito.any(PluginTask.class));
    }

//    -----------Verify configs --------------
    @Test
    public void testQueryByTagTypeConfigMissingTagType()
    {
        thrown.expect(ConfigException.class);
        thrown.expectMessage("tag_type and tag_value are required when query by Tag Type");
        ConfigSource config = baseConfig.set("query_by", Optional.of(QueryBy.TAG_TYPE)).set("tag_value", Optional.of("dummy"));
        mockPlugin.validateInputTask(mapTask(config));
    }

    @Test
    public void testQueryByTagTypeConfigMissingTagValue()
    {
        thrown.expect(ConfigException.class);
        thrown.expectMessage("tag_type and tag_value are required when query by Tag Type");
        ConfigSource config = baseConfig.set("query_by", Optional.of(QueryBy.TAG_TYPE)).set("tag_type", Optional.of("dummy"));
        mockPlugin.validateInputTask(mapTask(config));
    }

    @Test
    public void testQueryByDateRangeConfigMissingEarliestUpdatedAt()
    {
        thrown.expect(ConfigException.class);
        thrown.expectMessage("`earliest_updated_at` is required when query by Date Range");
        ConfigSource config = baseConfig.set("query_by", Optional.of(QueryBy.DATE_RANGE)).set("latest_updated_at", Optional.of(Date.from(OffsetDateTime.now(ZoneOffset.UTC).minusDays(10).toInstant())));
        mockPlugin.validateInputTask(mapTask(config));
    }

    @Test
    public void testQueryByDateRangeConfigMissingLatestUpdatedAt()
    {
        thrown.expect(ConfigException.class);
        thrown.expectMessage("`latest_updated_at` is required when query by Date Range");
        ConfigSource config = baseConfig.set("query_by", Optional.of(QueryBy.DATE_RANGE)).set("earliest_updated_at", Optional.of(Date.from(OffsetDateTime.now(ZoneOffset.UTC).minusDays(10).toInstant())));
        mockPlugin.validateInputTask(mapTask(config));
    }

    @Test
    public void testQueryByDateRangeConfigMissingLatestUpdatedAtNonIncremental()
    {
        thrown.expect(ConfigException.class);
        thrown.expectMessage("`latest_updated_at` is required when query by Date Range");
        ConfigSource config = baseConfig
                        .set("query_by", Optional.of(QueryBy.DATE_RANGE))
                        .set("incremental", Boolean.FALSE)
                        .set("earliest_updated_at", Optional.of(Date.from(OffsetDateTime.now(ZoneOffset.UTC).minusDays(10).toInstant())));
        mockPlugin.validateInputTask(mapTask(config));
    }

    @Test
    public void testNoErrorQueryByDateRangeConfigHasReportDurationNonIncremental()
    {
        ConfigSource config = baseConfig
                        .set("query_by", Optional.of(QueryBy.DATE_RANGE))
                        .set("report_duration", Optional.of(60L * 1000))
                        .set("incremental", Boolean.FALSE)
                        .set("earliest_updated_at", Optional.of(Date.from(OffsetDateTime.now(ZoneOffset.UTC).minusDays(10).toInstant())));
        mockPlugin.validateInputTask(mapTask(config));
    }

    @Test
    public void testNoErrorQueryByDateRangeConfigHasReportDuration()
    {
        ConfigSource config = baseConfig
                        .set("report_duration", Optional.of(60L * 1000))
                        .set("query_by", Optional.of(QueryBy.DATE_RANGE))
                        .set("earliest_updated_at", Optional.of(Date.from(OffsetDateTime.now(ZoneOffset.UTC).minusDays(10).toInstant())));
        mockPlugin.validateInputTask(mapTask(config));
    }

    @Test
    public void testQueryByDateRangeConfigHasEarliestUpdatedAtExceededNow()
    {
        OffsetDateTime earliestUpdatedAt = OffsetDateTime.now(ZoneOffset.UTC).plusDays(1);
        OffsetDateTime latestUpdatedAt = OffsetDateTime.now(ZoneOffset.UTC).minusDays(2);
        thrown.expect(ConfigException.class);
        thrown.expectMessage(String.format("`earliest_updated_at` (%s) cannot precede the current date ", earliestUpdatedAt.format(DATE_FORMATTER)));
        ConfigSource config = baseConfig
                        .set("query_by", Optional.of(QueryBy.DATE_RANGE))
                        .set("earliest_updated_at", Optional.of(Date.from(earliestUpdatedAt.toInstant())))
                        .set("latest_updated_at", Optional.of(Date.from(latestUpdatedAt.toInstant())));
        mockPlugin.validateInputTask(mapTask(config));
    }

    @Test
    public void testQueryByDateRangeConfigHasEarliestUpdatedAtExceededLatestUpdatedAt()
    {
        OffsetDateTime earliestUpdatedAt = OffsetDateTime.now(ZoneOffset.UTC).minusDays(10);
        OffsetDateTime latestUpdatedAt = OffsetDateTime.now(ZoneOffset.UTC).minusDays(20);
        thrown.expect(ConfigException.class);
        thrown.expectMessage(String.format("Invalid date range. `earliest_updated_at` (%s) cannot precede the `latest_updated_at` (%s).",
                        earliestUpdatedAt.format(DATE_FORMATTER),
                        latestUpdatedAt.format(DATE_FORMATTER)));

        ConfigSource config = baseConfig
                        .set("query_by", Optional.of(QueryBy.DATE_RANGE))
                        .set("earliest_updated_at", Optional.of(Date.from(earliestUpdatedAt.toInstant())))
                        .set("latest_updated_at", Optional.of(Date.from(latestUpdatedAt.toInstant())));
        mockPlugin.validateInputTask(mapTask(config));
    }

    @Test
    public void testHasFilterTypeButMissingFilterValue()
    {
        OffsetDateTime earliestUpdatedAt = OffsetDateTime.now(ZoneOffset.UTC).minusDays(20);
        OffsetDateTime latestUpdatedAt = OffsetDateTime.now(ZoneOffset.UTC).minusDays(10);
        thrown.expect(ConfigException.class);
        thrown.expectMessage("filter_value is required when selected filter_type");

        ConfigSource config = baseConfig
                        .set("query_by", Optional.of(QueryBy.DATE_RANGE))
                        .set("earliest_updated_at", Optional.of(Date.from(earliestUpdatedAt.toInstant())))
                        .set("latest_updated_at", Optional.of(Date.from(latestUpdatedAt.toInstant())))
                        .set("filter_type", Optional.of("dummy"));
        mockPlugin.validateInputTask(mapTask(config));
    }

    @Test
    public void testSkipIncrementalRunIfLastUpdatedAtExceedsNow()
    {
        OffsetDateTime earliestUpdatedAt = OffsetDateTime.now(ZoneOffset.UTC).minusDays(20);
        OffsetDateTime latestUpdatedAt = earliestUpdatedAt.plusDays(21);
        //21 days
        long reportDuration = 21 * 24 * 60 * 60 * 1000;

        ConfigSource config = baseConfig
                        .set("query_by", Optional.of(QueryBy.DATE_RANGE))
                        .set("earliest_updated_at", Optional.of(Date.from(earliestUpdatedAt.toInstant())))
                        .set("latest_updated_at", Optional.of(Date.from(latestUpdatedAt.toInstant())))
                        .set("report_duration", reportDuration)
                        .set("incremental", true);
        ServiceResponseMapper<? extends ValueLocator> mapper = mockPlugin.buildServiceResponseMapper(mapTask(config));
        RecordImporter recordImporter = mapper.createRecordImporter();
        PageBuilder mockPageBuilder = Mockito.mock(PageBuilder.class);
        TaskReport taskReport = mockPlugin.ingestServiceData(mapTask(config), recordImporter, 1, mockPageBuilder);
        // page builder object should never get called.
        Mockito.verify(mockPageBuilder, Mockito.never()).addRecord();

        String earliestUpdatedAtStr = taskReport.get(String.class, "earliest_updated_at");
        long duration = taskReport.get(Long.class, "report_duration");
        assertEquals(duration, reportDuration);
        assertEquals(earliestUpdatedAtStr, earliestUpdatedAt.format(DATE_FORMATTER));
    }

    @Test
    public void testBuildConfigDiff()
    {
        TaskReport taskReport1 = Mockito.mock(TaskReport.class);
        OffsetDateTime earliestUpdatedAt = OffsetDateTime.now(ZoneOffset.UTC).minusDays(20);
        OffsetDateTime latestUpdatedAt = OffsetDateTime.now(ZoneOffset.UTC).minusDays(10);
        ConfigSource config = baseConfig
                        .set("query_by", Optional.of(QueryBy.DATE_RANGE))
                        .set("earliest_updated_at", Optional.of(Date.from(earliestUpdatedAt.toInstant())))
                        .set("latest_updated_at", Optional.of(Date.from(latestUpdatedAt.toInstant())))
                        .set("incremental", true);

        ConfigDiff diff = mockPlugin.buildConfigDiff(mapTask(config), Mockito.mock(Schema.class), 1, Arrays.asList(taskReport1));

        long reportDuration = diff.get(Long.class, "report_duration");
        String nextEarliestUpdatedAt = diff.get(String.class, "earliest_updated_at");

        assertEquals(reportDuration, Duration.between(earliestUpdatedAt, latestUpdatedAt).toMillis());
        assertEquals(nextEarliestUpdatedAt, latestUpdatedAt.plusSeconds(1).format(DATE_FORMATTER));
    }

    @SuppressWarnings("unchecked")
    private void testRun(ConfigSource config, Predicate<MarketoRestClient> expectedCall) throws IOException
    {
        // Mock response data
        RecordPagingIterable<ObjectNode> mockRecordPagingIterable = Mockito.mock(RecordPagingIterable.class);
        JavaType javaType = OBJECT_MAPPER.getTypeFactory().constructParametrizedType(List.class, List.class, ObjectNode.class);
        List<ObjectNode> objectNodeList = OBJECT_MAPPER.readValue(this.getClass().getResourceAsStream("/fixtures/all_program_full.json"), javaType);
        Mockito.when(mockRecordPagingIterable.iterator()).thenReturn(objectNodeList.iterator());
        Mockito.when(mockRestClient.getProgramsByTag(Mockito.anyString(), Mockito.anyString())).thenReturn(mockRecordPagingIterable);
        Mockito.when(mockRestClient.getPrograms()).thenReturn(mockRecordPagingIterable);
        Mockito.when(mockRestClient.getProgramsByDateRange(
                      Mockito.any(Date.class),
                      Mockito.any(Date.class),
                      Mockito.nullable(String.class),
                      Mockito.nullable(List.class))).thenReturn(mockRecordPagingIterable);

        ServiceResponseMapper<? extends ValueLocator> mapper = mockPlugin.buildServiceResponseMapper(mapTask(config));
        RecordImporter recordImporter = mapper.createRecordImporter();
        PageBuilder mockPageBuilder = Mockito.mock(PageBuilder.class);
        mockPlugin.ingestServiceData(mapTask(config), recordImporter, 1, mockPageBuilder);

        // The method getProgramByTag is called 1 time
//        Mockito.verify(mockRestClient, Mockito.times(1)).getProgramsByTag(Mockito.anyString(), Mockito.anyString());
        expectedCall.test(mockRestClient);

        Schema embulkSchema = mapper.getEmbulkSchema();
        // 17 columns
        assertEquals(embulkSchema.size(), 17);
        // verify 3 times the method setLong for column id has been called
        ArgumentCaptor<Long> longArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        Mockito.verify(mockPageBuilder, Mockito.times(3)).setLong(Mockito.eq(embulkSchema.lookupColumn("id")), longArgumentCaptor.capture());
        List<Long> allValues = longArgumentCaptor.getAllValues();
        assertArrayEquals(new Long[]{1004L, 1001L, 1003L}, allValues.toArray());
    }

    @Test
    public void testRunQueryByTagType() throws IOException
    {
        ConfigSource config = baseConfig
                        .set("query_by", Optional.of(QueryBy.TAG_TYPE))
                        .set("tag_type", Optional.of("dummy"))
                        .set("tag_value", Optional.of("dummy"));
        Predicate<MarketoRestClient> expectedCall = mockRest -> {
            Mockito.verify(mockRest, Mockito.times(1)).getProgramsByTag(Mockito.anyString(), Mockito.anyString());
            return true;
        };
        testRun(config, expectedCall);
    }

    @Test
    public void testRunWithoutQueryBy() throws IOException
    {
        Predicate<MarketoRestClient> expectedCall = input -> {
            Mockito.verify(input, Mockito.times(1)).getPrograms();
            return true;
        };
        testRun(baseConfig, expectedCall);
    }

    @Test
    public void testRunQueryByDateRange() throws IOException
    {
        OffsetDateTime earliestUpdatedAt = OffsetDateTime.now(ZoneOffset.UTC).minusDays(20);
        OffsetDateTime latestUpdatedAt = OffsetDateTime.now(ZoneOffset.UTC).minusDays(10);
        ConfigSource config = baseConfig
                        .set("query_by", Optional.of(QueryBy.DATE_RANGE))
                        .set("earliest_updated_at", Optional.of(Date.from(earliestUpdatedAt.toInstant())))
                        .set("latest_updated_at", Optional.of(Date.from(latestUpdatedAt.toInstant())));
        Predicate<MarketoRestClient> expectedCall = input -> {
            Mockito.verify(input, Mockito.times(1)).getProgramsByDateRange(
                            Mockito.any(Date.class),
                            Mockito.any(Date.class),
                            Mockito.nullable(String.class),
                            Mockito.nullable(List.class));
            return true;
        };
        testRun(config, expectedCall);
    }

    private PluginTask mapTask(ConfigSource config)
    {
        return CONFIG_MAPPER.map(config, PluginTask.class);
    }

    public ConfigSource config() throws IOException
    {
        ConfigLoader configLoader = runtime.getInjector().getInstance(ConfigLoader.class);
        return configLoader.fromYaml(this.getClass().getResourceAsStream("/config/rest_config.yaml"));
    }
}
