class BasicUpdatePlanner
  attr_reader :metadata_manager

  def initialize(metadata_manager)
    @metadata_manager = metadata_manager
  end

  def execute_delete(delete_data, buffer_pool_manager)
    plan = TablePlan.new(buffer_pool_manager, delete_data.table_name, metadata_manager)
    select_plan = SelectPlan.new(plan, delete_data.predicate)
    update_scan = select_plan.open

    count = 0
    while update_scan.next
      update_scan.delete
      count += 1
    end

    count
  end

  def execute_modify(modify_data, buffer_pool_manager)
    plan = TablePlan.new(buffer_pool_manager, modify_data.table_name, metadata_manager)
    select_plan = SelectPlan.new(plan, modify_data.predicate)
    update_scan = select_plan.open

    count = 0
    while update_scan.next
      value = modify_data.new_value.evaluate(update_scan)
      update_scan.set_value(modify_data.field_name, value)
      count += 1
    end

    count
  end

  def execute_insert(insert_data, buffer_pool_manager)
    plan = TablePlan.new(buffer_pool_manager, insert_data.table_name, metadata_manager)

    # 今回の実装の場合、plan.openでTableScanを都度生成すると先頭のページに書き込みしてしまう
    # その結果、常に同じページへデータが書き込まれてしまうことから、それを回避するためにキャッシュしておく
    @update_scan ||= plan.open
    @update_scan.insert

    insert_data.field_list.zip(insert_data.constant_list) do |field_name, value|
      @update_scan.set_value(field_name, value)
    end

    1
  end

  def execute_create_table(create_table_data)
    metadata_manager.create_table(create_table_data.table_name, create_table_data.schema)

    0
  end

  def execute_create_view(create_view_data, buffer_pool_manager)
    metadata_manager.create_view(create_view_data.view_name, create_view_data.view_definition, buffer_pool_manager)

    0
  end

  def execute_create_index(create_index_data, buffer_pool_manager)
    metadata_manager.create_index(create_index_data.index_name, create_index_data.table_name, buffer_pool_manager)

    0
  end
end