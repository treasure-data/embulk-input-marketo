require "embulk/input/marketo_api/soap/base"

module Embulk
  module Input
    module MarketoApi
      module Soap
        class Lead < Base
          def metadata
            # http://developers.marketo.com/documentation/soap/describemobject/
            response = savon_call(:describe_m_object, message: {object_name: "LeadRecord"})
            response.body[:success_describe_m_object][:result][:metadata][:field_list][:field]
          end

          def each(last_updated_at, &block)
            # http://developers.marketo.com/documentation/soap/getmultipleleads/

            last_updated_at = Time.parse(last_updated_at).iso8601

            # TODO: generate request in #fetch
            # TODO: use PREVIEW_COUNT as batch_size in preview
            request = {
              lead_selector: {
                oldest_updated_at: last_updated_at,
              },
              attributes!: {
                lead_selector: {"xsi:type" => "ns1:LastUpdateAtSelector"}
              },
              batch_size: 1000,
            }

            stream_position = fetch(request, &block)

            while stream_position
              stream_position = fetch(request.merge(stream_position: stream_position), &block)
            end
          end

          private

          def fetch(request = {}, &block)
            response = savon_call(:get_multiple_leads, message: request)

            remaining = response.xpath('//remainingCount').text.to_i
            Embulk.logger.info "Remaining records: #{remaining}"
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

            if remaining > 0
              response.xpath('//newStreamPosition').text
            else
              nil
            end
          end
        end
      end
    end
  end
end
