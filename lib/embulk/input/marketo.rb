require "savon"

module Embulk
  module Input
    class Marketo < InputPlugin
      Plugin.register_input("marketo", self)

      def self.transaction(config, &control)
        # configuration code:
        task = {
          "property1" => config.param("property1", :string),
          "property2" => config.param("property2", :integer, default: 0),
        }

        columns = [
          Column.new(0, "example", :string),
          Column.new(1, "column", :long),
          Column.new(2, "value", :double),
        ]

        resume(task, columns, 1, &control)
      end

      def self.resume(task, columns, count, &control)
        commit_reports = yield(task, columns, count)

        next_config_diff = {}
        return next_config_diff
      end

      def self.guess(config)
        client = generate_soap_client(config)
        metadata_response = client.call(:describe_m_object, message: {object_name: "LeadRecord"})
        metadata = metadata_response.body[:success_describe_m_object][:result][:metadata][:field_list][:field]

        return {"columns" => generate_columns(metadata)}
      end

      def self.generate_soap_client(config)
        endpoint_url = config.param(:endpoint, :string)
        wsdl_url = config.param(:wsdl, :string, default: "#{endpoint_url}?WSDL")
        user_id = config.param(:user_id, :string)
        encryption_key = config.param(:encryption_key, :string)

        signature = self.generate_signature(endpoint_url, user_id, encryption_key)
        headers = {
          'ns1:AuthenticationHeader' => {
            "mktowsUserId" => user_id,
          }.merge(signature)
        }

        client = Savon.client(
          wsdl: wsdl_url,
          soap_header: headers,
          endpoint: endpoint_url,
          open_timeout: 10,
          read_timeout: 300,
          namespace_identifier: :ns1,
          env_namespace: 'SOAP-ENV'
        )

        client
      end

      def self.generate_signature(endpoint, user_id, encryption_key)
        timestamp = Time.now.to_s
        encryption_string = timestamp + user_id
        digest = OpenSSL::Digest.new('sha1')
        hashed_signature = OpenSSL::HMAC.hexdigest(digest, encryption_key, encryption_string)

        {'requestTimestamp' => timestamp, 'requestSignature' => hashed_signature.to_s}
      end

      def self.generate_columns(metadata)
        metadata.map do |field|
          type =
            case field[:data_type]
            when "integer"
              "long"
            when "dateTime", "date"
              "timestamp"
            when "string", "text", "phone", "currency"
              "string"
            when "boolean"
              "boolean"
            else
              "string"
            end

          {name: field[:name], type: type}
        end
      end

      def init
        # initialization code:
        @property1 = task["property1"]
        @property2 = task["property2"]
      end

      def run
        page_builder.add(["example-value", 1, 0.1])
        page_builder.add(["example-value", 2, 0.2])
        page_builder.finish

        commit_report = {}
        return commit_report
      end
    end
  end
end
