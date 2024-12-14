# frozen_string_literal: true

module SimpleRubyDb
  module Metadata
    class TableManager
      MAX_NAME = 16

      attr_accessor :field_catalog_layout
      attr_reader :metadata_buffer_pool_manager

      def initialize(is_new, metadata_buffer_pool_manager)
        # Slotを使っていないことから、 table_catalog テーブルは作成不要
        field_catalog_schema = SimpleRubyDb::Record::Schema.new
        field_catalog_schema.add_string_field('table_name', MAX_NAME)
        field_catalog_schema.add_string_field('field_name', MAX_NAME)
        field_catalog_schema.add_string_field('field_type', MAX_NAME)
        field_catalog_schema.add_int_field('length')
        field_catalog_schema.add_int_field('offset')
        @field_catalog_layout = SimpleRubyDb::Record::Layout.new(field_catalog_schema)
        @metadata_buffer_pool_manager = metadata_buffer_pool_manager

        create_table('field_catalog', field_catalog_schema) if is_new
      end

      def create_table(table_name, schema)
        create_field_catalog(table_name, schema)
      end

      private def create_field_catalog(table_name, schema)
        layout = SimpleRubyDb::Record::Layout.new(schema)
        table_scan = SimpleRubyDb::Record::TableScan.new(
          metadata_buffer_pool_manager, 'field_catalog', field_catalog_layout)
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
        schema = SimpleRubyDb::Record::Schema.new
        offsets = {}
        table_scan = SimpleRubyDb::Record::TableScan.new(
          metadata_buffer_pool_manager, 'field_catalog', field_catalog_layout)

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

        SimpleRubyDb::Record::Layout.new(schema, offsets: offsets)
      end
    end
  end
end


