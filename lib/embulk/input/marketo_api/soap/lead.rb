require "embulk/input/marketo_api/soap/base"

module Embulk
  module Input
    module MarketoApi
      module Soap
        class Lead < Base
          # NOTE: batch_size is allowed at 1000, but that takes 2 minutes in 1 request.
          #       We use 250 for the default (about 30 seconds)
          BATCH_SIZE_DEFAULT = 250

          def metadata
            # http://developers.marketo.com/documentation/soap/describemobject/
            response = savon_call(:describe_m_object, message: {object_name: "LeadRecord"})
            response.body[:success_describe_m_object][:result][:metadata][:field_list][:field]
          end

          def each(range, options = {}, &block)
            # http://developers.marketo.com/documentation/soap/getmultipleleads/
            request = {
              lead_selector: {
                oldest_updated_at: range[:from].iso8601,
                latest_updated_at: range[:to].iso8601,
              },
              attributes!: {
                lead_selector: {"xsi:type" => "ns1:LastUpdateAtSelector"}
              },
              batch_size: options[:batch_size] || BATCH_SIZE_DEFAULT,
            }
            Embulk.logger.info "Fetching from '#{range[:from]}' to '#{range[:to]}'..."

            stream_position = fetch(request, &block)

            while stream_position
              stream_position = fetch(request.merge(stream_position: stream_position), &block)
            end
          end

          private

          def fetch(request = {}, &block)
            start = Time.now
            response = savon_call(:get_multiple_leads, message: request)
            Embulk.logger.info "Fetched in #{Time.now - start} seconds"

            records = response.xpath('//leadRecordList/leadRecord')
            remaining = response.xpath('//remainingCount').text.to_i
            Embulk.logger.info "Fetched records in the range: #{records.size}"
            Embulk.logger.info "Remaining records in the range: #{remaining}"
            records.each do |lead|
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
