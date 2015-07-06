require "savon"

module Embulk
  module Input
    module MarketoApi
      class Soap
        attr_reader :endpoint, :wsdl, :user_id, :encryption_key

        def initialize(endpoint, wsdl, user_id, encryption_key)
          @endpoint = endpoint
          @wsdl = wsdl
          @user_id = user_id
          @encryption_key = encryption_key
        end

        def lead_metadata
          # http://developers.marketo.com/documentation/soap/describemobject/
          response = savon.call(:describe_m_object, message: {object_name: "LeadRecord"})
          response.body[:success_describe_m_object][:result][:metadata][:field_list][:field]
        end

        def each_lead(last_updated_at, &block)
          # http://developers.marketo.com/documentation/soap/getmultipleleads/

          last_updated_at = Time.parse(last_updated_at).iso8601
          request = {
            lead_selector: {
              oldest_updated_at: last_updated_at,
            },
            attributes!: {
              lead_selector: {"xsi:type" => "ns1:LastUpdateAtSelector"}
            },
            batch_size: 100,
          }

          stream_position = fetch_leads(request, &block)

          while stream_position
            stream_position = fetch_leads(request.merge(stream_position: stream_position), &block)
          end
        end

        private

        def fetch_leads(request = {}, &block)
          response = savon.call(:get_multiple_leads, message: request)

          response.xpath('//leadRecordList/leadRecord').each do |lead|
            record = {
              "id" => {type: :integer, value: lead.xpath('Id').text.to_i},
              "email" => {type: :string, value: lead.xpath('Email').text}
            }
            lead.xpath('leadAttributeList/attribute').each do |attr|
              name = attr.xpath('attrName').text
              type = attr.xpath('attrType').text
              value = attr.xpath('attrValue').text
              record = record.merge(
                name => {
                  type: type,
                  value: value
                }
              )
            end

            block.call(record)
          end

          if response.xpath('//remainingCount').text.to_i > 0
            response.xpath('//newStreamPosition').text
          else
            nil
          end
        end

        def savon
          @savon ||=
            begin
              headers = {
                'ns1:AuthenticationHeader' => {
                  "mktowsUserId" => user_id,
                }.merge(signature)
              }
              Savon.client(
                wsdl: wsdl,
                soap_header: headers,
                endpoint: endpoint,
                open_timeout: 10,
                read_timeout: 300,
                raise_errors: true,
                namespace_identifier: :ns1,
                env_namespace: 'SOAP-ENV'
              )
            end
        end

        def signature
          timestamp = Time.now.to_s
          encryption_string = timestamp + user_id
          digest = OpenSSL::Digest.new('sha1')
          hashed_signature = OpenSSL::HMAC.hexdigest(digest, encryption_key, encryption_string)
          {
            'requestTimestamp' => timestamp,
            'requestSignature' => hashed_signature.to_s
          }
        end
      end
    end
  end
end
