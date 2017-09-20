package org.embulk.input.marketo;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.spi.Column;

import java.io.File;
import java.util.Date;
import java.util.List;

/**
 * Created by tai.khuu on 9/6/17.
 */
public interface MarketoService
{
    List<Column> describeLead();

    List<Column> describeLeadByProgram();

    List<Column> describeLeadByLists();

    File extractLead(Date startTime, Date endTime, List<String> extractFields, int pollingTimeIntervalSecond, int bulkJobTimeoutSecond);

    File extractAllActivity(Date startTime, Date endTime, int pollingTimeIntervalSecond, int bulkJobTimeoutSecond);

    Iterable<ObjectNode> getAllListLead(List<String> extractFields);

    Iterable<ObjectNode> getAllProgramLead(List<String> extractFields);

    Iterable<ObjectNode> getCampaign();
}
