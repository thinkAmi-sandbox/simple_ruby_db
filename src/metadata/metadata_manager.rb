require_relative './table_manager'
require_relative './view_manager'
require_relative './statistical_manager'

class MetadataManager
  attr_reader :table_manager
  attr_reader :view_manager
  attr_reader :statistical_manager
  attr_reader :index_manager

  def initialize(is_new, metadata_buffer_pool_manager)
    # TODO metadataを作るときにエラーとなるので、metadataなしで進められないか検討する
    #
    @table_manager = TableManager.new(is_new, metadata_buffer_pool_manager)
    # @view_manager = ViewManager.new(is_new, table_manager, buffer_pool_manager)
    # @statistical_manager = StatisticalManager.new(table_manager, buffer_pool_manager)

    # TODO index作るまでは一旦様子見
    # @index_manager = IndexManager.new(is_new, table_manager, statistical_manager, buffer_pool_manager)
  end

  def create_table(table_name, schema)
    table_manager.create_table(table_name, schema)
  end
  
  def layout(table_name)
    table_manager.layout(table_name)
  end

  def create_view(view_name, view_def, buffer_pool_manager)
    view_manager.create_view(view_name, view_def, buffer_pool_manager)
  end

  def view_definition(view_name, buffer_pool_manager)
    # TODO view_managerは作ってないので、コメントアウトしている
    # view_manager.view_definition(view_name, buffer_pool_manager)
  end

  def create_index(index_name, table_name, field_name, buffer_pool_manager)
    index_manager.create_index(index_name, table_name, field_name, buffer_pool_manager)
  end

  def index_info(table_name, buffer_pool_manager)
    index_manager.index_info(table_name, buffer_pool_manager)
  end

  def stat_info(table_name, layout, buffer_pool_manager)
    statistical_manager.stat_info(table_name, layout, buffer_pool_manager)
  end
end
