require "embulk/input/marketo_api/soap/base"

module Embulk
  module Input
    module MarketoApi
      module Soap
        class ActivityLog < Base
          def metadata(last_updated_at, options={})
            activity_logs = []

            fetch_by_last_updated_at(last_updated_at, options) do |record|
              activity_logs << record
            end

            Guess::SchemaGuess.from_hash_records(activity_logs)
          end

          def each(last_updated_at, options={}, &block)
            response = fetch_by_last_updated_at(last_updated_at, options, &block)

            while response.is_a?(String) do
              response = fetch_by_offset(response, options, &block)
            end

            response
          end

          private

          def fetch_by_last_updated_at(last_updated_at, options={}, &block)
            last_updated_at = last_updated_at.to_s
            last_updated_at = Time.parse(last_updated_at).iso8601

            request = {
              start_position: {
                oldest_created_at: last_updated_at,
              },
            }

            fetch(request, options, &block)
          end

          def fetch_by_offset(offset, options={}, &block)
            request = {
              start_position: {
                offset: offset,
              },
            }

            fetch(request, options, &block)
          end

          def fetch(request, options={}, &block)
            request[:batch_size] = options[:batch_size] || 100

            response = savon.call(:get_lead_changes, message: request)
            remaining = response.body[:success_get_lead_changes][:result][:remaining_count].to_i
            Embulk.logger.info "Remaining records: #{remaining}"

            activities = response.body[:success_get_lead_changes][:result][:lead_change_record_list][:lead_change_record]
            activities.each do |activity|
              record = {
                "id" => activity[:id],
                # embulk can't treat DateTime
                "activity_date_time" => activity[:activity_date_time].to_time,
                "activity_type" => activity[:activity_type],
                "mktg_asset_name" => activity[:mktg_asset_name],
                "mkt_person_id" => activity[:mkt_person_id],
              }

              activity[:activity_attributes][:attribute].each do |attributes|
                name = attributes[:attr_name]
                value = attributes[:attr_value]

                record[name] = value
              end

              block.call(record)
            end

            if remaining > 0
              response.body[:success_get_lead_changes][:result][:new_start_position][:offset]
            else
              activities
            end
          end
        end
      end
    end
  end
end

