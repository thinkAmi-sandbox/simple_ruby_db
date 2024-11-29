require_relative './../parse/parser'

class Planner
  attr_reader :query_planner
  attr_reader :update_planner

  def initialize(query_planner, update_planner)
    @query_planner = query_planner
    @update_planner = update_planner
  end

  def create_query_plan(query, buffer_pool_manager)
    parser = Parser.new(query)
    query_data = parser.query
    verify_query(query_data)

    query_planner.create_plan(query_data, buffer_pool_manager)
  end

  def execute_update(command, buffer_pool_manager)
    parser = Parser.new(command)
    data = parser.dml
    verify_update(data)

    case data
    when InsertData
      update_planner.execute_insert(data, buffer_pool_manager)
    when DeleteData
      update_planner.execute_delete(data, buffer_pool_manager)
    when ModifyData
      update_planner.execute_modify(data, buffer_pool_manager)
    when CreateTableData
      update_planner.execute_create_table(data, buffer_pool_manager)
    when CreateViewData
      update_planner.execute_create_view(data, buffer_pool_manager)
    when CreateIndexData
      update_planner.execute_create_index(data, buffer_pool_manager)
    else
      0
    end
  end

  private def verify_query(query_data)
    # SimpleDBは query の検証を行わないが、メソッド定義は必要
  end

  private def verify_update(update_data)
    # SimpleDBは update の検証を行わないが、メソッド定義は必要
  end
end