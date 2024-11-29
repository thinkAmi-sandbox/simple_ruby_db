require_relative './table_plan'
require_relative './product_plan'
require_relative './project_plan'
require_relative './select_plan'
require_relative './plan_creatable'

class BasicQueryPlanner
  include PlanCreatable

  attr_reader :metadata_manager

  def initialize(metadata_manager)
    @metadata_manager = metadata_manager
  end

  def create_plan(query_data, buffer_pool_manager)
    # Step 1: Create a plan for each mentioned table or view.
    plans = create_plans(query_data.table_list, buffer_pool_manager)

    # Step 2: Create the product of all table plans
    product_plan = plans.reduce { |plan, next_plan| ProductPlan.new(plan, next_plan) }

    # Step 3: Add a selection plan for the predicate
    select_plan = SelectPlan.new(product_plan, query_data.predicate)

    # Step 4: Project on the field names
    ProjectPlan.new(select_plan, query_data.field_list)
  end
end