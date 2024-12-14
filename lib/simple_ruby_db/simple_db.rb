# frozen_string_literal: true

module SimpleRubyDb
  class SimpleDb
    attr_reader :disk_manager
    attr_reader :buffer_pool_manager
    attr_reader :metadata_disk_manager
    attr_reader :metadata_buffer_pool_manager
    # log_managerは無い
    attr_reader :metadata_manager
    attr_reader :planner

    def initialize(heap_file_path, metadata_file_path, query_planner: 'basic')
      @disk_manager = SimpleRubyDb::Disk::DiskManager.open(heap_file_path)
      buffer_pool = SimpleRubyDb::Buffer::BufferPool.new(1)
      @buffer_pool_manager = SimpleRubyDb::Buffer::BufferPoolManager.new(disk_manager, buffer_pool)

      @metadata_disk_manager = SimpleRubyDb::Disk::DiskManager.open(metadata_file_path)
      metadata_buffer_pool = SimpleRubyDb::Buffer::BufferPool.new(1)
      @metadata_buffer_pool_manager = SimpleRubyDb::Buffer::BufferPoolManager.new(
        metadata_disk_manager, metadata_buffer_pool)

      @metadata_manager = SimpleRubyDb::Metadata::MetadataManager.new(true, metadata_buffer_pool_manager)
      query_planner = query_planner == 'basic' ?
                        SimpleRubyDb::Plan::BasicQueryPlanner.new(metadata_manager) :
                        SimpleRubyDb::Plan::BetterQueryPlanner.new(metadata_manager)
      update_planner = SimpleRubyDb::Plan::BasicUpdatePlanner.new(metadata_manager)

      @planner = SimpleRubyDb::Plan::Planner.new(query_planner, update_planner)
    end
  end
end
