module DynamoDBOperation

  # expr(:UserID, :eq, {userID: "MyValue"}) 
  #   return [ "UserID = :userID", {':userID' => "MyValue" } ]
  #
  # expr( expr(:UserID, :eq, {userID: "MyValue"}),
  #      :and
  #       expr(:FileID, :eq, {fileID: "MyFileID"}))
  #  return [ "UserID = :userID AND FileID = :fileID", {':userID' => "MyValue", ':fileID' => 'MyFileID'}]
  def expr(left_expr, operation, right_expr)
    lambda { 
      left = nil
      values = {}
      case left_expr
      when Symbol, String
        left = left_expr
      else #lambda
        left, values = left_expr.call
      end

      right = nil
      right_values = {}
      case right_expr
      when Hash
        right = ":#{right_expr.keys[0]}"
        right_values = {":#{right_expr.keys[0]}" => right_expr.values[0]}
      else #lambda
        right, right_values = right_expr.call
      end
      values.merge!(right_values)

      ["#{left.to_s} #{operation} #{right.to_s}", values]}
  end

  # dynamo_eq(:UserID, {userID: "MyValue"})
  #
  # dynamo_and( dynamo_eq(:UserID, {userID: "MyValue"}),
  #             dynamo_eq(:fileID, {fileID: "MyFileID"}) )
  {dynamo_eq: '=', dynamo_and: 'AND'}.each do |name, operator|
    define_method(name.to_s) do |left_expr, right_expr|
      expr(left_expr, operator, right_expr)
    end
  end
end
