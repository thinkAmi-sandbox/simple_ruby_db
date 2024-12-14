# frozen_string_literal: true

require_relative '../test_helper'

module Disk
  class DiskTest < Minitest::Test
    def test_disk_manager
      temp_file = Tempfile.open(mode: 32770)
      disk_manager = SimpleRubyDb::Disk::DiskManager.new(temp_file)

      hello = Array.new(SimpleRubyDb::Disk::DiskManager::PAGE_SIZE)
      hello[0, 5] = 'hello'.bytes
      hello.fill(0, hello.size...SimpleRubyDb::Disk::DiskManager::PAGE_SIZE)
      # 要素がnilだとpackする時にエラーとなるため、 0 へと変換しておく
      hello = hello.map { |v| v.nil? ? 0 : v }
      hello_page_id = disk_manager.allocate_page
      disk_manager.write_page_data(hello_page_id, hello)
      # 今の作りでは、強制的に書き出す処理を明示する必要がある
      disk_manager.sync

      world = Array.new(SimpleRubyDb::Disk::DiskManager::PAGE_SIZE)
      world[0, 5] = 'world'.bytes
      world.fill(0, world.size...SimpleRubyDb::Disk::DiskManager::PAGE_SIZE)
      world = world.map { |v| v.nil? ? 0 : v }
      world_page_id = disk_manager.allocate_page
      disk_manager.write_page_data(world_page_id, world)
      disk_manager.sync

      disk_manager2 = SimpleRubyDb::Disk::DiskManager.open(temp_file.path)
      buffer_hello = disk_manager2.read_page_data(hello_page_id)
      assert_equal hello, buffer_hello

      buffer_world = disk_manager2.read_page_data(world_page_id)
      assert_equal world, buffer_world
    end
  end
end
