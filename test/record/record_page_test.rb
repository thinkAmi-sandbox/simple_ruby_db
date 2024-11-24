require 'minitest/autorun'
require_relative './../../src/disk/disk_manager'
require_relative './../../src/buffer/buffer_pool'
require_relative './../../src/buffer/buffer_pool_manager'
require_relative './../../src/record/layout'
require_relative './../../src/record/schema'
require_relative './../../src/record/record_page'

class RecordPageTest < Minitest::Test
  def test_record_page
    temp_file = Tempfile.open(mode: 32770)
    disk_manager = DiskManager.new(temp_file)
    buffer_pool = BufferPool.new(1)
    buffer_manager = BufferPoolManager.new(disk_manager, buffer_pool)
    buffer = buffer_manager.create_page

    schema = Schema.new
    schema.add_int_field('int_field_name')
    schema.add_string_field('string_field_name', 9)
    schema.add_int_field('int_field_name2')

    layout = Layout.new(schema)
    record_page = RecordPage.new(buffer_manager, buffer.page_id, layout)

    record_page.set_int('int_field_name', 123)
    actual = record_page.get_int('int_field_name')

    assert_equal 123, actual

    record_page.set_string('string_field_name', 'hello')
    actual = record_page.get_string('string_field_name')

    assert_equal 'hello', actual

    record_page.set_int('int_field_name2', 101)
    actual = record_page.get_int('int_field_name2')

    assert_equal 101, actual
  end
end
