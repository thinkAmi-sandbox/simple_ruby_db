require_relative './../query/select_scan'

class SelectPlan
  attr_reader :plan
  attr_reader :predicate

  def initialize(plan, predicate)
    @plan = plan
    @predicate = predicate
  end

  def open
    scan = plan.open
    SelectScan.new(scan, predicate)
  end

  def block_accessed
    plan.block_accessed
  end

  def records_output
    plan.records_output / predicate.reduction_factor(plan)
  end

  def distinct_values(field_name)
    return 1 unless predicate.equals_with_constant(field_name).nil?

    other_filed_name = predicate.equals_with_field(field_name)
    return if other_filed_name.nil? plan.distinct_values(field_name)

    [plan.distinct_values(field_name), plan.distinct_values(other_filed_name)].min
  end

  def schema
    plan.schema
  end
end