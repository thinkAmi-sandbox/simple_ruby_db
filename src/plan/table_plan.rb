require_relative '../record/table_scan'

class TablePlan
  attr_reader :table_name
  attr_reader :buffer_pool_manager
  attr_reader :layout

  # TODO 今のところ用意しない
  attr_reader :stat_info

  def initialize(buffer_pool_manager, table_name, metadata_manager)
    @table_name = table_name
    @buffer_pool_manager = buffer_pool_manager

    @layout = metadata_manager.layout(table_name, buffer_pool_manager)

    # TODO 今のところは実装しない
    # stat_info = metadata_manager.stat_info
  end

  def open
    TableScan.new(buffer_pool_manager, table_name, layout)
  end

  def block_accessed
    # TODO stat_infoが必要なので様子見
  end

  def records_output
    # TODO stat_infoが必要なので様子見
  end

  def distinct_values(field_name)
    # TODO stat_infoが必要なので様子見
  end

  def schema
    layout.schema
  end
end