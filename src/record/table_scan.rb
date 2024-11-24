require_relative './../query/constant'
class TableScan
  attr_reader :buffer_pool_manager
  attr_reader :table_name
  attr_reader :layout
  attr_accessor :record_page

  def initialize(buffer_pool_manager, table_name, layout)
    @buffer_pool_manager = buffer_pool_manager
    @table_name = table_name
    @layout = layout
    @record_page = nil

    if File.size(buffer_pool_manager.disk_manager.heap_file) == 0
      move_to_new_page
    else
      move_to_page(0)
    end
  end

  def before_first
    move_to_page(0)
  end

  def next
    return false if at_last_page?

    move_to_page(record_page.page_id + 1)
    true
  end

  def get_int(field_name)
    record_page.get_int(field_name)
  end

  def get_string(field_name)
    record_page.get_string(field_name)
  end

  def value(field_name)
    return Constant.new(get_int(field_name)) if layout.schema.field_type(field_name) == :integer
    Constant.new(get_string(field_name))
  end

  def has_field?(field_name)
    layout.schema.has_field?(field_name)
  end

  def set_int(field_name, value)
    record_page.set_int(field_name, value)
  end

  def set_string(field_name, value)
    record_page.set_string(field_name, value)
  end

  def set_value(field_name, value)
    set_int(field_name, value.int_value) if layout.schema.field_type(field_name) == :integer
    set_string(field_name, value.string_value)
  end

  def insert
    if at_last_page?
      move_to_new_page
    else
      move_to_page(record_page.page_id + 1)
    end
  end

  def delete
    # slotは使ってないので、使おうとしたらエラーになるようにしておく
    raise
  end

  def move_to_new_page
    # pinは使ってないので、 closeは不要

    new_page = buffer_pool_manager.create_page
    @record_page = RecordPage.new(buffer_pool_manager, new_page.page_id, layout)
    record_page.format
  end

  def move_to_page(page_id)
    # pinは使ってないので、 closeは不要

    @record_page = RecordPage.new(buffer_pool_manager, page_id, layout)
  end

  def at_last_page?
    # 発行済みのページIDの最後になっているかを確認する
    record_page.page_id == buffer_pool_manager.disk_manager.next_page_id - 1
  end
end