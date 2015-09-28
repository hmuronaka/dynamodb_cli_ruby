class DynamoDBTable

  class << self

    def table_name(name = nil)
      if name
        @table_name = name
      else
        @table_name
      end
    end

    def index_name(name = nil)
      if name
        @index_name = name
      else
        @index_name
      end
    end

    def fields(fields = nil)
      if fields.nil?
        @fields
      else
        @fields = fields
        @fields.each do |f|
          attr_accessor f
        end
      end
    end

    def return_consumed_capacity(value = nil)
      if value.nil?
        @return_consumed_capacity
      else
        @return_consumed_capacity = value
      end
    end

    def scan(db_client, option={})
      resp = db_client.scan(
        table_name: self.table_name,
        return_consumed_capacity: _return_consumed_capacity(option)
      )
      
      resp.items = resp.items.map do |item|
        self.new(item)
      end
      resp
    end

    # expr(:UserID, :eq, {userID=>userID})
    def query(db_client, expr, option={})
      key_condition_expression, expression_attribute_values = expr.call

      params = {
        table_name: self.table_name,
        return_consumed_capacity: _return_consumed_capacity(option),
        key_condition_expression: key_condition_expression,
        expression_attribute_values: expression_attribute_values
      }

      params[:index_name] = option[:index_name] if option[:index_name]

      resp = db_client.query(params)
      resp.items = resp.items.map do |item|
        self.new(item)
      end
      resp
    end

    private 
    def _return_consumed_capacity(option)
      option[:return_consumed_capacity] || self.return_consumed_capacity || "INDEXES"
    end
  end

  def initialize(item)
    item.each do |key, value|
      self[key] = value
    end
  end

  def [](key)
    self.send("#{key.to_s}")
  end

  def []=(key, value)
    self.send("#{key.to_s}=", value)
  end

  def to_header_s
    strs = self.class.fields.map do |key|
      key
    end.join(', ')
  end

  def to_s
    strs = self.class.fields.map do |key|
      self[key].to_s
    end.join(', ')
  end

#     strs = self.class.fields.map do |key|
#       "\t#{key}: #{self[key].to_s}"
#     end.join("\n")
#     strs =<<EOS
# {
# #{strs}
# }
# EOS
#   end
end
