# frozen_string_literal: true

require_relative '../test_helper'

module Record
  class TableScanTest < Minitest::Test
    def test_table_scan
      temp_file = Tempfile.open(mode: 32770)
      disk_manager = SimpleRubyDb::Disk::DiskManager.new(temp_file)
      buffer_pool = SimpleRubyDb::Buffer::BufferPool.new(1)
      buffer_manager = SimpleRubyDb::Buffer::BufferPoolManager.new(disk_manager, buffer_pool)

      schema = SimpleRubyDb::Record::Schema.new
      schema.add_int_field('int_field_name')
      schema.add_string_field('string_field_name', 9)
      schema.add_int_field('int_field_name2')

      layout = SimpleRubyDb::Record::Layout.new(schema)
      table_scan = SimpleRubyDb::Record::TableScan.new(buffer_manager, 'T', layout)

      (0..9).each do |i|
        table_scan.insert

        table_scan.set_int('int_field_name', i)
        table_scan.set_string('string_field_name', "val#{i}")
      end

      table_scan.before_first

      # TableScan.newで create_pageするので、 page_id = 0 には空のデータが入っている
      # そのため、 next で次の page_id を読むようにしている
      table_scan.next  # page_id == 1
      actual = table_scan.get_int('int_field_name')
      assert_equal 0, actual
      actual = table_scan.get_string('string_field_name')
      assert_equal 'val0', actual

      table_scan.next  # page_id == 2
      actual = table_scan.get_int('int_field_name')
      assert_equal 1, actual
      actual = table_scan.get_string('string_field_name')
      assert_equal 'val1', actual

      table_scan.move_to_page(4)
      actual = table_scan.get_int('int_field_name')
      assert_equal 3, actual
      actual = table_scan.get_string('string_field_name')
      assert_equal 'val3', actual
    end
  end
end

