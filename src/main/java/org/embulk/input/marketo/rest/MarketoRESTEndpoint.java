package org.embulk.input.marketo.rest;

import org.apache.commons.lang3.text.StrSubstitutor;

import java.util.Map;

/**
 * Created by tai.khuu on 9/5/17.
 */
public enum MarketoRESTEndpoint
{
    ACCESS_TOKEN("/oauth/token"),
    CREATE_LEAD_EXTRACT("/bulk/v1/leads/export/create.json"),
    CREATE_ACTIVITY_EXTRACT("/bulk/v1/activities/export/create.json"),
    DESCRIBE_LEAD("/rest/v1/leads/describe.json"),
    START_LEAD_EXPORT_JOB("/bulk/v1/leads/export/${export_id}/enqueue.json"),
    START_ACTIVITY_EXPORT_JOB("/bulk/v1/activities/export/${export_id}/enqueue.json"),
    GET_ACTIVITY_EXPORT_STATUS("/bulk/v1/activities/export/${export_id}/status.json"),
    GET_LEAD_EXPORT_STATUS("/bulk/v1/leads/export/${export_id}/status.json"),
    GET_LEAD_EXPORT_RESULT("/bulk/v1/leads/export/${export_id}/file.json"),
    GET_ACTIVITY_EXPORT_RESULT("/bulk/v1/activities/export/${export_id}/file.json"),
    GET_LISTS("/rest/v1/lists.json"),
    GET_LEADS_BY_LIST("/rest/v1/lists/${list_id}/leads.json"),
    GET_PROGRAMS("/rest/asset/v1/programs.json"),
    GET_LEADS_BY_PROGRAM("/rest/v1/leads/programs/${program_id}.json"),
    GET_CAMPAIGN("/rest/v1/campaigns.json"),
    GET_PROGRAMS_BY_TAG("/rest/asset/v1/program/byTag.json"),
    GET_CUSTOM_OBJECT("/rest/v1/customobjects/${api_name}.json"),
    GET_CUSTOM_OBJECT_DESCRIBE("/rest/v1/customobjects/${api_name}/describe.json"),
    GET_ACTIVITY_TYPES("/rest/v1/activities/types.json"),
    DESCRIBE_PROGRAM_MEMBERS("/rest/v1/programs/members/describe.json"),
    CREATE_PROGRAM_MEMBERS_EXPORT_JOB("/bulk/v1/program/members/export/create.json"),
    START_PROGRAM_MEMBERS_EXPORT_JOB("/bulk/v1/program/members/export/${export_id}/enqueue.json"),
    GET_PROGRAM_MEMBERS_EXPORT_STATUS("/bulk/v1/program/members/export/${export_id}/status.json"),
    GET_PROGRAM_MEMBERS_EXPORT_RESULT("/bulk/v1/program/members/export/${export_id}/file.json");
    private final String endpoint;

    MarketoRESTEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
    }

    public String getEndpoint()
    {
        return endpoint;
    }

    public String getEndpoint(Map<String, String> pathParams)
    {
        return StrSubstitutor.replace(endpoint, pathParams);
    }
}
