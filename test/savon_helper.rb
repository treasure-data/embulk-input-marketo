module SavonHelper
  private

  def savon_response(body)
    globals = {
      namespace_identifier: :ns1,
      env_namespace: 'SOAP-ENV',
    }
    httpi = HTTPI::Response.new(200, {}, body)
    Savon::Response.new(httpi, globals.merge(default_nori_options), {advanced_typecasting: false})
  end

  def default_nori_options
    # https://github.com/savonrb/savon/blob/v2.11.1/lib/savon/options.rb#L75-L94
    {
      :strip_namespaces          => true,
      :convert_tags_to  => lambda { |tag| tag.snakecase.to_sym},
      :convert_attributes_to     => lambda { |k,v| [k,v] },
    }
  end
end
