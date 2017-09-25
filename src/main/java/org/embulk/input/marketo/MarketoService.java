package org.embulk.input.marketo;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.input.marketo.model.MarketoField;

import java.io.File;
import java.util.Date;
import java.util.List;

/**
 * Created by tai.khuu on 9/6/17.
 */
public interface MarketoService
{
    List<MarketoField> describeLead();

    List<MarketoField> describeLeadByProgram();

    List<MarketoField> describeLeadByLists();

    File extractLead(Date startTime, Date endTime, List<String> extractedFields, int pollingTimeIntervalSecond, int bulkJobTimeoutSecond);

    File extractAllActivity(Date startTime, Date endTime, int pollingTimeIntervalSecond, int bulkJobTimeoutSecond);

    Iterable<ObjectNode> getAllListLead(List<String> extractFields);

    Iterable<ObjectNode> getAllProgramLead(List<String> extractFields);

    Iterable<ObjectNode> getCampaign();
}
