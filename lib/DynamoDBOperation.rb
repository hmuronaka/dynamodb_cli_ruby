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
      left, left_values = parse_left_expr(left_expr)

      right, right_values = parse_right_expr(right_expr)

      values = left_values.merge(right_values)

      ["#{left.to_s} #{operation} #{right.to_s}", values]
    }
  end

  # dynamo_eq(:UserID, {userID: "MyValue"})
  #
  # dynamo_and( dynamo_eq(:UserID, {userID: "MyValue"}),
  #             dynamo_eq(:fileID, {fileID: "MyFileID"}) )
  {dynamo_eq: '=', 
   dynamo_not_eq: '<>',
   dynamo_lt: '<',
   dynamo_gt: '>',
   dynamo_le: '<=',
   dynamo_ge: '>=',
   dynamo_not: 'NOT',
   dynamo_and: 'AND',
   dynamo_or: 'OR'}.each do |name, operator|
     define_method(name.to_s) do |left_expr, right_expr|
       expr(left_expr, operator, right_expr)
     end
   end

   def dynamo_between(attribute_name, left_expr, right_expr)
     lambda {
       left, left_values = parse_left_expr(left_expr)
       right, right_values = parse_right_expr(right_expr)

       values = left_values.merge(right_values)

       ["#{attribute_name} between #{left.to_s} and #{right.to_s}", values]
     }
   end

   def dynamo_in(attribute_name, *exprs)
     lambda {
       expr_str = []
       values = []
       exprs.each do |expr|
         temp_str, temp_values = parse_right_expr(expr)
         expr_str << temp_str
         values << temp_values
       end

       ["#{attribute_name} IN(#{expr_str.join(', ')})", values]
     }
   end

private
   def parse_left_expr(left_expr)
      left = nil
      values = {}
      case left_expr
      when Symbol, String
        left = left_expr
      else #lambda
        left, values = left_expr.call
      end

      [left, values]
   end

   private
   def parse_right_expr(right_expr)
     right = nil
     values = {}
     case right_expr
     when Hash
       right = ":#{right_expr.keys[0]}"
       values = {":#{right_expr.keys[0]}" => right_expr.values[0]}
     when String,Symbol
       right = ":#{right_expr.to_s}"
       values = {}
     else #lambda
       right, values = right_expr.call
     end

     [right, values]
   end



end
