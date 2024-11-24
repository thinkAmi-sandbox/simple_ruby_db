require_relative './../disk/disk_manager'

class Buffer
  attr_accessor :page_id
  attr_accessor :page
  attr_accessor :is_dirty

  def initialize(page_id, page, is_dirty)
    @page_id = page_id
    @page = page
    @is_dirty = is_dirty
  end

  def dirty?
    is_dirty
  end

  def self.create
    new(0, Array.new(DiskManager::PAGE_SIZE, 0), false)
  end
end