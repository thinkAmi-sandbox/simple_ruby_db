# frozen_string_literal: true

module SimpleRubyDb
  module Plan
    module PlanCreatable
      def create_plans(table_list, buffer_pool_manager)
        [].tap do |plans|
          table_list.each do |table_name|
            # TODO Viewのmetadataは作ってないので、常にtableを検索することになる
            view_definition = metadata_manager.view_definition(table_name, buffer_pool_manager)
            if view_definition.nil?
              plans.push(TablePlan.new(buffer_pool_manager, table_name, metadata_manager))
            else
              # TODO Viewの場合の処理を追加
            end
          end
        end
      end
    end
  end
end