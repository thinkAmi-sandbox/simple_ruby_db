require 'minitest/autorun'
require_relative './../../src/disk/disk_manager'
require_relative './../../src/buffer/buffer_pool'
require_relative './../../src/buffer/buffer_pool_manager'
require_relative './../../src/record/layout'
require_relative './../../src/record/schema'
require_relative './../../src/record/record_page'
require_relative './../../src/record/table_scan'

class TableScanTest < Minitest::Test
  def test_table_scan
    temp_file = Tempfile.open(mode: 32770)
    disk_manager = DiskManager.new(temp_file)
    buffer_pool = BufferPool.new(1)
    buffer_manager = BufferPoolManager.new(disk_manager, buffer_pool)

    schema = Schema.new
    schema.add_int_field('int_field_name')
    schema.add_string_field('string_field_name', 9)
    schema.add_int_field('int_field_name2')

    layout = Layout.new(schema)
    table_scan = TableScan.new(buffer_manager, 'T', layout)

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