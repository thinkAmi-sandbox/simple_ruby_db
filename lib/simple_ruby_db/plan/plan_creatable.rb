# frozen_string_literal: true

module SimpleRubyDb
  module Plan
    module PlanCreatable
      def create_plans(table_list, buffer_pool_manager)
        table_list.map do |table_name|
          # Viewのmetadataは作ってないので、常にtableを検索することになる
          TablePlan.new(buffer_pool_manager, table_name, metadata_manager)
        end
      end
    end
  end
end