require "savon_helper"

module ActivityLogFixtures
  include SavonHelper

  private

  def activity_log_xml(body)
    <<XML
<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:ns1="http://www.marketo.com/mktows/">
  <SOAP-ENV:Body>
    <ns1:successGetLeadChanges>
      <result>
        #{body}
      </result>
    </ns1:successGetLeadChanges>
  </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
XML
  end

  def xml_ac_response_no_attributes
    activity_log_xml <<XML
<returnCount>2</returnCount>
<remainingCount>0</remainingCount>
<newStartPosition>
  <latestCreatedAt>2015-07-14T09:13:10+09:00</latestCreatedAt>
  <oldestCreatedAt>2015-07-14T09:13:13+09:00</oldestCreatedAt>
  <activityCreatedAt xsi:nil="true"/>
  <offset>offset</offset>
</newStartPosition>
<leadChangeRecordList>
  <leadChangeRecord>
    <id>1</id>
    <activityDateTime>2015-07-14T09:00:09+09:00</activityDateTime>
    <activityType>at1</activityType>
    <mktgAssetName>score1</mktgAssetName>
    <activityAttributes>
      <attribute>
        <attrName>Attribute Name</attrName>
        <attrType xsi:nil="true"/>
        <attrValue>Attribute1</attrValue>
      </attribute>
      <attribute>
        <attrName>Old Value</attrName>
        <attrType xsi:nil="true"/>
        <attrValue>402</attrValue>
      </attribute>
    </activityAttributes>
    <mktPersonId>100</mktPersonId>
  </leadChangeRecord>
  <leadChangeRecord>
    <id>2</id>
    <activityDateTime>2015-07-14T09:00:10+09:00</activityDateTime>
    <activityType>at2</activityType>
    <mktgAssetName>score2</mktgAssetName>
    <mktPersonId>90</mktPersonId>
  </leadChangeRecord>
</leadChangeRecordList>
XML
  end


  def xml_ac_response
    activity_log_xml <<XML
<returnCount>2</returnCount>
<remainingCount>1</remainingCount>
<newStartPosition>
  <latestCreatedAt>2015-07-14T09:13:10+09:00</latestCreatedAt>
  <oldestCreatedAt>2015-07-14T09:13:13+09:00</oldestCreatedAt>
  <activityCreatedAt xsi:nil="true"/>
  <offset>offset</offset>
</newStartPosition>
<leadChangeRecordList>
  <leadChangeRecord>
    <id>1</id>
    <activityDateTime>2015-07-14T09:00:09+09:00</activityDateTime>
    <activityType>at1</activityType>
    <mktgAssetName>score1</mktgAssetName>
    <activityAttributes>
      <attribute>
        <attrName>Attribute Name</attrName>
        <attrType xsi:nil="true"/>
        <attrValue>Attribute1</attrValue>
      </attribute>
      <attribute>
        <attrName>Old Value</attrName>
        <attrType xsi:nil="true"/>
        <attrValue>402</attrValue>
      </attribute>
    </activityAttributes>
    <mktPersonId>100</mktPersonId>
  </leadChangeRecord>
  <leadChangeRecord>
    <id>2</id>
    <activityDateTime>2015-07-14T09:00:10+09:00</activityDateTime>
    <activityType>at2</activityType>
    <mktgAssetName>score2</mktgAssetName>
    <activityAttributes>
      <attribute>
        <attrName>Attribute Name</attrName>
        <attrType xsi:nil="true"/>
        <attrValue>Attribute2</attrValue>
      </attribute>
      <attribute>
        <attrName>Old Value</attrName>
        <attrType xsi:nil="true"/>
        <attrValue>403</attrValue>
      </attribute>
    </activityAttributes>
    <mktPersonId>90</mktPersonId>
  </leadChangeRecord>
</leadChangeRecordList>
XML
  end

  def xml_ac_next_response
    activity_log_xml <<XML
<returnCount>1</returnCount>
<remainingCount>0</remainingCount>
<newStartPosition>
</newStartPosition>
<leadChangeRecordList>
  <leadChangeRecord>
    <id>3</id>
    <activityDateTime>2015-07-14T09:00:11+09:00</activityDateTime>
    <activityType>at3</activityType>
    <mktgAssetName>score3</mktgAssetName>
    <activityAttributes>
      <attribute>
        <attrName>Attribute Name</attrName>
        <attrType xsi:nil="true"/>
        <attrValue>Attribute3</attrValue>
      </attribute>
      <attribute>
        <attrName>Old Value</attrName>
        <attrType xsi:nil="true"/>
        <attrValue>404</attrValue>
      </attribute>
    </activityAttributes>
    <mktPersonId>100</mktPersonId>
  </leadChangeRecord>
</leadChangeRecordList>
XML
  end

  def xml_ac_none_response
    activity_log_xml <<XML
<returnCount>0</returnCount>
<remainingCount>0</remainingCount>
<newStartPosition>
</newStartPosition>
<leadChangeRecordList>
</leadChangeRecordList>
XML
  end

  def xml_ac_preview_response
    activity_log_xml <<XML
<returnCount>15</returnCount>
<remainingCount>0</remainingCount>
<newStartPosition>
</newStartPosition>
<leadChangeRecordList>
#{xml_ac_attr(15)}
</leadChangeRecordList>
XML
  end

  def xml_ac_attr(times = 15)
    response = ""
    (1..times).each do |n|
    response << <<-XML
  <leadChangeRecord>
    <id>#{n}</id>
    <activityDateTime>2015-07-14T00:00:11+00:00</activityDateTime>
    <activityType>at#{n}</activityType>
    <mktgAssetName>score#{n}</mktgAssetName>
    <activityAttributes>
      <attribute>
        <attrName>Attribute Name</attrName>
        <attrType xsi:nil="true"/>
        <attrValue>Attribute#{n}</attrValue>
      </attribute>
      <attribute>
        <attrName>Old Value</attrName>
        <attrType xsi:nil="true"/>
        <attrValue>404</attrValue>
      </attribute>
    </activityAttributes>
    <mktPersonId>100</mktPersonId>
  </leadChangeRecord>
    XML
    end
    response
  end

  def offset
    "offset"
  end
end
