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
            while response[:remaining_count] > 0 do
              offset = response[:offset]
              response = fetch_by_offset(offset, options, &block)
            end

            response[:last_updated_at]
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

            activities_list = response.body[:success_get_lead_changes][:result][:lead_change_record_list]

            if activities_list.nil?
              Embulk.logger.info "No record is fetched."
              return {remaining_count: 0, offset: nil, last_updated_at: nil}
            end

            activities = activities_list[:lead_change_record].sort { |activity| activity[:activity_date_time] }

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

            {
              remaining_count: remaining,
              offset: response.body[:success_get_lead_changes][:result][:new_start_position][:offset],
              last_updated_at: activities.last[:activity_date_time]
            }
          end
        end
      end
    end
  end
end

