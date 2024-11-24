require_relative './../disk/disk_manager'

class BufferPoolManager
  attr_reader :disk_manager
  attr_reader :buffer_pool

  def initialize(disk_manager, buffer_pool)
    @disk_manager = disk_manager
    @buffer_pool = buffer_pool
    @page_table = {}
  end

  def fetch_page(page_id)
    buffer_id = @page_table[page_id]
    unless buffer_id.nil?
      frame = @buffer_pool[buffer_id]
      frame.usage_count += 1

      return frame.buffer
    end

    buffer_id = @buffer_pool.evict
    frame = @buffer_pool[buffer_id]
    evict_page_id = frame.buffer.page_id

    buffer = frame.buffer
    if buffer.dirty?
      disk_manager.write_page_data(evict_page_id, buffer.page)
      disk_manager.sync
    end

    buffer.page_id = page_id
    buffer.is_dirty = false
    buffer.page = disk_manager.read_page_data(page_id)

    frame.usage_count = 1

    @page_table.delete(evict_page_id)
    @page_table[page_id] = buffer_id

    frame.buffer
  end

  def create_page
    buffer_id = @buffer_pool.evict
    frame = @buffer_pool[buffer_id]
    evict_page_id = frame.buffer.page_id

    buffer = frame.buffer
    if buffer.dirty?
      disk_manager.write_page_data(evict_page_id, buffer.page)
      disk_manager.sync
    end

    page_id = disk_manager.allocate_page

    # bufferはそのままで、ページを書き換える
    buffer.page = Array.new(DiskManager::PAGE_SIZE, 0)

    buffer.page_id = page_id
    buffer.is_dirty = true
    frame.usage_count = 1

    @page_table.delete(evict_page_id)
    @page_table[page_id] = buffer_id

    frame.buffer
  end

  def flush
    @page_table.each do |page_id, buffer_id|
      frame = buffer_pool[buffer_id]
      page = frame.buffer.page
      disk_manager.write_page_data(page_id, page)
      frame.buffer.is_dirty = false
    end

    disk_manager.sync
  end
end