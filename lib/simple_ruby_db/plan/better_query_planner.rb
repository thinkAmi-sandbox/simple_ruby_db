# frozen_string_literal: true

module SimpleRubyDb
  module Plan
    class BetterQueryPlanner
      include PlanCreatable

      private attr_reader :metadata_manager

      def initialize(metadata_manager)
        @metadata_manager = metadata_manager
      end

      def create_plan(query_data, buffer_pool_manager)
        # Step 1: Create a plan for each mentioned table or view.
        plans = create_plans(query_data.table_list, buffer_pool_manager)

        # Step 2: Create the product of all table plans
        product_plan = plans.reduce do |plan, next_plan|
          choice1 = ProductPlan.new(plan, next_plan)
          choice2 = ProductPlan.new(next_plan, plan)

          choice1.block_accessed < choice2.block_accessed ? choice1 : choice2
        end

        # Step 3: Add a selection plan for the predicate
        select_plan = SelectPlan.new(product_plan, query_data.predicate)

        # Step 4: Project on the field names
        ProjectPlan.new(select_plan, query_data.fields)
      end
    end
  end
end