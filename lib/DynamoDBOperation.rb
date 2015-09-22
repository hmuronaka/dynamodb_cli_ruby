module DynamoDBOperation

  # expr(:UserID, :eq, {userID: "MyValue"}) 
  #   return [ "UserID = :userID", {':userID' => "MyValue" } ]
  #
  # expr( expr(:UserID, :eq, {userID: "MyValue"}),
  #      :and
  #       expr(:FileID, :eq, {fileID: "MyFileID"}))
  #  return [ "UserID = :userID AND FileID = :fileID", {':userID' => "MyValue", ':fileID' => 'MyFileID'}]
  def expr(left_operand, operation, right_operand)
    lambda { 
      operation_str = string_from_operation(operation)

      left = nil
      values = {}
      case left_operand
      when Symbol, String
        left = left_operand
      else #lambda
        left, values = left_operand.call
      end

      right = nil
      right_values = {}
      case right_operand
      when Hash
        right = ":#{right_operand.keys[0]}"
        right_values = {":#{right_operand.keys[0]}" => right_operand.values[0]}
      else #lambda
        right, right_values = right_operand.call
      end
      values.merge!(right_values)

      ["#{left.to_s} #{operation_str} #{right.to_s}", values]}
  end

  private
  def string_from_operation(operation)
    case operation
    when :eq
      return "="
    when :and
      return "AND"
    end
  end

end

