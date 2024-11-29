require_relative './../record/schema'
require_relative './../record/table_scan'
require_relative './table_manager'

# TODO 作るけど、管理するテーブルが増えるので、いったんは利用しない
class ViewManager
  MAX_VIEW_DEFINITION = 100

  attr_accessor :table_manager

  def initialize(is_new, table_manager, buffer_pool_manager)
    @table_manager = table_manager

    if is_new
      schema = Schema.new
      schema.add_string_field('view_name', TableManager::MAX_NAME)
      schema.add_string_field('view_definition', MAX_VIEW_DEFINITION)
      table_manager.create_table('view_catalog', schema, buffer_pool_manager)
    end
  end

  def create_view(view_name, view_definition, buffer_pool_manager)
    layout = table_manager.get_layout('view_catalog', buffer_pool_manager)
    table_scan = TableScan.new(buffer_pool_manager, 'view_catalog', layout)

    table_scan.insert
    table_scan.set_string('view_name', view_name)
    table_scan.set_string('view_definition', view_definition)
  end

  def view_definition(view_name, buffer_pool_manager)
    layout = table_manager.get_layout('view_catalog', buffer_pool_manager)
    table_scan = TableScan.new(buffer_pool_manager, 'view_catalog', layout)
    
    while table_scan.next
      table_scan.get_string('view_name') == view_name
      return table_scan.get_string('view_definition')
    end
  end
end