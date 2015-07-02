module LeadFixtures
  private

  def leads_response
    Nokogiri::XML(raw_response)
  end

  def next_stream_leads_response
    Nokogiri::XML(raw_next_stream_response)
  end

  def raw_response
<<XML
<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:ns1="http://www.marketo.com/mktows/">
  <SOAP-ENV:Body>
    <ns1:successGetMultipleLeads>
      <result>
        <returnCount>2</returnCount>
        <remainingCount>1</remainingCount>
        <newStreamPosition>#{stream_position}</newStreamPosition>
        <leadRecordList>
          <leadRecord>
            <Id>65835</Id>
            <Email>manyo@marbketo.com</Email>
            <ForeignSysPersonId xsi:nil="true" />
            <ForeignSysType xsi:nil="true" />
            <leadAttributeList>
              <attribute>
                <attrName>Name</attrName>
                <attrType>string</attrType>
                <attrValue>manyo</attrValue>
              </attribute>
            </leadAttributeList>
          </leadRecord>
          <leadRecord>
            <Id>67508</Id>
            <Email>everyleaf@marketo.com</Email>
            <ForeignSysPersonId xsi:nil="true" />
            <ForeignSysType xsi:nil="true" />
            <leadAttributeList>
              <attribute>
                <attrName>Name</attrName>
                <attrType>string</attrType>
                <attrValue>everyleaf</attrValue>
              </attribute>
            </leadAttributeList>
          </leadRecord>
        </leadRecordList>
      </result>
    </ns1:successGetMultipleLeads>
  </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
XML
  end

  def stream_position
    "next_steam_position"
  end


  def raw_next_stream_response
<<XML
<?xml version="1.0" encoding="UTF-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:ns1="http://www.marketo.com/mktows/">
  <SOAP-ENV:Body>
    <ns1:successGetMultipleLeads>
      <result>
        <returnCount>2</returnCount>
        <remainingCount>0</remainingCount>
        <newStreamPosition />
        <leadRecordList>
          <leadRecord>
            <Id>65835</Id>
            <Email>ten-thousand-leaf@marketo.com</Email>
            <ForeignSysPersonId xsi:nil="true" />
            <ForeignSysType xsi:nil="true" />
            <leadAttributeList>
              <attribute>
                <attrName>Name</attrName>
                <attrType>string</attrType>
                <attrValue>ten-thousand-leaf</attrValue>
              </attribute>
            </leadAttributeList>
          </leadRecord>
        </leadRecordList>
      </result>
    </ns1:successGetMultipleLeads>
  </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
XML
  end
end
