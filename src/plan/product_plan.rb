require_relative './../record/schema'
require_relative './../query/product_scan'

class ProductPlan
  attr_reader :left_hand_subquery_plan
  attr_reader :right_hand_subquery_plan
  attr_reader :schema

  def initialize(left_hand_subquery_plan, right_hand_subquery_plan)
    @left_hand_subquery_plan = left_hand_subquery_plan
    @right_hand_subquery_plan = right_hand_subquery_plan

    @schema = Schema.new.tap do |schema|
      schema.add_all(left_hand_subquery_plan)
      schema.add_all(right_hand_subquery_plan)
    end
  end

  def open
    scan_left = left_hand_subquery_plan.open
    scan_right = right_hand_subquery_plan.open

    ProductScan.new(scan_left, scan_right)
  end

  def block_accessed
    left_hand_subquery_plan.block_accessed + (left_hand_subquery_plan.records_output * right_hand_subquery_plan.records_output)
  end

  def records_output
    left_hand_subquery_plan.records_output * right_hand_subquery_plan.records_output
  end

  def distinct_values(field_name)
    return left_hand_subquery_plan.distinct_values(field_name) if left_hand_subquery_plan.has_field?(field_name)
    right_hand_subquery_plan.distinct_values(field_name)
  end
end