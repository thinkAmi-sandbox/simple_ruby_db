# frozen_string_literal: true

module SimpleRubyDb
  module Metadata
    class MetadataManager
      attr_reader :table_manager

      def initialize(is_new, metadata_buffer_pool_manager)
        @table_manager = TableManager.new(is_new, metadata_buffer_pool_manager)
      end

      def create_table(table_name, schema)
        table_manager.create_table(table_name, schema)
      end

      def layout(table_name)
        table_manager.layout(table_name)
      end
    end
  end
end


