module SavonHelper
  private

  def savon_response(body)
    globals = {
      namespace_identifier: :ns1,
      env_namespace: 'SOAP-ENV',

      # https://github.com/savonrb/savon/blob/v2.11.1/lib/savon/options.rb#L75-L94
      strip_namespaces:         true,
      convert_response_tags_to: lambda { |tag| tag.snakecase.to_sym},
      convert_attributes_to:    lambda { |k,v| [k,v] },
    }
    httpi = HTTPI::Response.new(200, {}, body)
    Savon::Response.new(httpi, globals, {advanced_typecasting: false})
  end
end
