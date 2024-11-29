class IndexInfo
  private attr_reader :index_name
  private attr_reader :field_name
  private attr_reader :table_schema
  private attr_reader :buffer_pool_manager
  private attr_reader :index_layout
  private attr_reader :stat_info

  def initialize(index_name, field_name, table_schema, buffer_pool_manager, stat_info)
    @index_name = index_name
    @field_name = field_name
    @table_schema = table_schema
    @buffer_pool_manager = buffer_pool_manager
    @index_layout = create_index_layout
    @stat_info = stat_info
  end

  def open
    # TODO hash index を作ったら実装する
  end

  def block_accessed
    # TODO hash index を作ったらまともに実装する
    1
  end

  def records_output
    stat_info.records_output / stat_info.distinct_values(field_name)
  end

  def distinct_values(f_name)
    field_name == f_name ? 1 : stat_info.distinct_values(field_name)
  end

  private def create_index_layout
    schema = Schema.new
    schema.add_int_field('block')
    schema.add_int_field('id')

    if schema.field_type(field_name) == :integer
      length = table_schema.length(field_name)
      schema.add_string_field('dataval', length)
    end

    Layout.new(schema)
  end
end