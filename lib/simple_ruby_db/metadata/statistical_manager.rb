# frozen_string_literal: true

module SimpleRubyDb
  module Metadata
    class StatisticalManager
      private attr_reader :table_manager
      private attr_reader :table_statistical
      private attr_accessor :number_of_calls

      def initialize(table_manager, buffer_pool_manager)
        raise 'This is a placeholder implementation.'

        @table_manager = table_manager
        refresh_statistics(buffer_pool_manager)
      end

      def stat_info(table_name, layout, buffer_pool_manager)
        number_of_calls += 1

        if number_of_calls > 100
          refresh_statistics(buffer_pool_manager)
        end

        result = table_statistical[table_name]
        if result.nil?
          stat = calculate_table_stats(table_name, layout, buffer_pool_manager)
          table_statistical[table_name] = stat
        end

        result
      end

      def refresh_statistics(buffer_pool_manager)
        table_stats = {}

        table_layout = table_manager.layout('table_catalog', buffer_pool_manager)
        table_scan = TableScan.new(buffer_pool_manager, 'table_catalog', table_layout)

        while table_scan.next
          table_name = table_scan.get_string('table_name')
          layout = table_manager.layout(table_name, buffer_pool_manager)
          stat_info = calculate_table_stats(table_name, layout, buffer_pool_manager)
          table_stats[table_name] = stat_info
        end
      end

      private def calculate_table_stats(table_name, layout, buffer_pool_manager)
        num_of_records = 0
        num_of_blocks = 0

        table_scan = TableScan.new(buffer_pool_manager, table_name, layout)

        while table_scan.next
          num_of_records += 1

          # TODO num_of_blocksの使い道がでてきたときに、改めて実装する
          # num_of_blocks = table_scan.rid.block_number
        end

        StatInfo.new(num_of_blocks, num_of_records)
      end
    end
  end
end