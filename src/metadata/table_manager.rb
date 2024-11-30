require_relative './../record/layout'
require_relative './../record/schema'

class TableManager
  MAX_NAME = 16

  # attr_accessor :table_catalog_layout
  attr_accessor :field_catalog_layout
  attr_reader :metadata_buffer_pool_manager

  def initialize(is_new, metadata_buffer_pool_manager)
    # table_catalog_schema = Schema.new
    # table_catalog_schema.add_string_field('table_name', MAX_NAME)
    # table_catalog_schema.add_int_field('slot_size')
    # @table_catalog_layout = Layout.new(table_catalog_schema)

    field_catalog_schema = Schema.new
    field_catalog_schema.add_string_field('table_name', MAX_NAME)
    field_catalog_schema.add_string_field('field_name', MAX_NAME)
    field_catalog_schema.add_string_field('field_type', MAX_NAME)  # Rubyのシンボルで定義しているためstring型
    field_catalog_schema.add_int_field('length')
    field_catalog_schema.add_int_field('offset')
    @field_catalog_layout = Layout.new(field_catalog_schema)
    @metadata_buffer_pool_manager = metadata_buffer_pool_manager

    if is_new
      # create_table('table_catalog', table_catalog_schema, buffer_pool_manager)
      create_table('field_catalog', field_catalog_schema)
    end
  end

  def create_table(table_name, schema)
    # create_table_catalog(table_name, schema, buffer_pool_manager)
    create_field_catalog(table_name, schema)
  end

  # private def create_table_catalog(table_name, schema, buffer_pool_manager)
  #   layout = Layout.new(schema)
  #   table_scan = TableScan.new(buffer_pool_manager, 'table_catalog', layout)
  #   table_scan.insert
  #   table_scan.set_string('table_name', table_name)
  #   table_scan.set_int('slot_size', layout.slot_size)
  # end

  private def create_field_catalog(table_name, schema)
    layout = Layout.new(schema)
    table_scan = TableScan.new(metadata_buffer_pool_manager, 'field_catalog', field_catalog_layout)
    schema.fields.each do |field_name|
      table_scan.insert
      table_scan.set_string('table_name', table_name)
      table_scan.set_string('field_name', field_name)
      table_scan.set_string('field_type', schema.field_type(field_name))
      table_scan.set_int('length', schema.field_length(field_name))
      table_scan.set_int('offset', layout.offset(field_name))
    end
  end

  def layout(table_name)
    schema = Schema.new
    offsets = {}
    table_scan = TableScan.new(metadata_buffer_pool_manager, 'field_catalog', field_catalog_layout)

    while table_scan.next
      if table_scan.get_string('table_name') == table_name
        field_name = table_scan.get_string('field_name')
        field_type = table_scan.get_string('field_type')
        field_length = table_scan.get_int('length')
        field_offset = table_scan.get_int('offset')

        offsets[field_name] = field_offset
        schema.add_field(field_name, field_type, field_length)
      end
    end

    Layout.new(schema, offsets: offsets)
  end
end