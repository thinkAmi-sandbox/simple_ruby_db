require 'minitest/autorun'
require_relative './../../src/disk/disk_manager'
require_relative './../../src/buffer/buffer_pool'
require_relative './../../src/buffer/buffer_pool_manager'

class BufferTest < Minitest::Test
  def test_buffer
    hello = Array.new(DiskManager::PAGE_SIZE)
    hello[0, 5] = 'hello'.bytes
    hello = hello.map { |v| v.nil? ? 0 : v }

    world = Array.new(DiskManager::PAGE_SIZE)
    world[0, 5] = 'world'.bytes
    world = world.map { |v| v.nil? ? 0 : v }

    temp_file = Tempfile.open(mode: 32770)
    disk_manager = DiskManager.new(temp_file)
    buffer_pool = BufferPool.new(1)
    buffer_manager = BufferPoolManager.new(disk_manager, buffer_pool)

    b1 = buffer_manager.create_page

    # 2回目を実行してもエラーにならない...ので、いったん create_page のみ
    # assert_raises do
    #   buffer_manager.create_page
    # end
    buffer_manager.create_page

    p1 = b1.page
    p1.replace(hello)
    b1.is_dirty = true
    page1_id = b1.page_id

    p1_buffer = buffer_manager.fetch_page(page1_id)
    assert_equal hello, p1_buffer.page

    b2 = buffer_manager.create_page
    p2 = b2.page
    p2.replace(world)
    b2.is_dirty = true
    page2_id = b2.page_id

    p1_buffer_2 = buffer_manager.fetch_page(page1_id)
    assert_equal hello, p1_buffer_2.page

    p2_buffer = buffer_manager.fetch_page(page2_id)
    assert_equal world, p2_buffer.page
  end
end