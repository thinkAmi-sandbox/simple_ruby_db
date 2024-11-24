class Term
  attr_reader :lhs_expression
  attr_reader :rhs_expression

  def initialize(lhs_expression, rhs_expression)
    @lhs_expression = lhs_expression
    @rhs_expression = rhs_expression
  end

  def satisfied?(scan)
    lhs_value = lhs_expression.evaluate(scan)
    rhs_value = rhs_expression.evaluate(scan)

    lhs_value == rhs_value
  end

  def reduction_factor(plan)
    if lhs_expression.field_name? && rhs_expression.field_name?
      lhs_name = lhs_expression.field_name
      rhs_name = rhs_expression.field_name

      return [plan.distinct_values(lhs_name), plan.distinct_values(rhs_name)].max
    end

    if lhs_expression.field_name?
      return plan.distinct_values(lhs_expression.field_name)
    end

    if rhs_expression.field_name?
      return plan.distinct_values(rhs_expression.field_name)
    end

    if lhs_expression.value == rhs_expression.value
      return 1
    end

    Integer::MAX
  end

  def equals_with_constant(field_name)
    if lhs_expression.field_name? && lhs_expression.field_name == field_name && !rhs_expression.field_name?
      rhs_expression.value
    elsif rhs_expression.field_name? && rhs_expression.field_name == field_name && !lhs_expression.field_name?
      lhs_expression.value
    end
  end

  def equals_with_field(field_name)
    if lhs_expression.field_name? && lhs_expression.field_name == field_name && rhs_expression.field_name?
      rhs_expression.field_name
    elsif rhs_expression.field_name? && rhs_expression.field_name == field_name && lhs_expression.field_name?
      lhs_expression.field_name
    end
  end

  def applies_to?(schema)
    lhs_expression.applies_to?(schema) && rhs_expression.applies_to?(schema)
  end

  def to_s
    "#{lhs_expression.to_s}=#{rhs_expression.to_s}"
  end
end