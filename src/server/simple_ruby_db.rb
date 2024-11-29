require_relative './../disk/disk_manager'
require_relative './../buffer/buffer_pool'
require_relative './../buffer/buffer_pool_manager'
require_relative './../metadata/metadata_manager'
require_relative './../plan/basic_query_planner'
require_relative './../plan/better_query_planner'
require_relative './../plan/basic_update_planner'
require_relative './../plan/planner'

class SimpleRubyDb
  attr_reader :disk_manager
  attr_reader :buffer_pool_manager
  attr_reader :metadata_disk_manager
  attr_reader :metadata_buffer_pool_manager
  # log_managerは無い
  attr_reader :metadata_manager
  attr_reader :planner

  def initialize(heap_file_path, metadata_file_path, query_planner: 'basic')
    @disk_manager = DiskManager.open(heap_file_path)
    buffer_pool = BufferPool.new(1)
    @buffer_pool_manager = BufferPoolManager.new(disk_manager, buffer_pool)

    @metadata_disk_manager = DiskManager.open(metadata_file_path)
    metadata_buffer_pool = BufferPool.new(1)
    @metadata_buffer_pool_manager = BufferPoolManager.new(metadata_disk_manager, metadata_buffer_pool)

    @metadata_manager = MetadataManager.new(true, metadata_buffer_pool_manager)
    query_planner = query_planner == 'basic' ?
                      BasicQueryPlanner.new(metadata_manager) :
                      BetterQueryPlanner.new(metadata_manager)
    update_planner = BasicUpdatePlanner.new(metadata_manager)

    @planner = Planner.new(query_planner, update_planner)
  end
end