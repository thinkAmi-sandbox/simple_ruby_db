require 'minitest/autorun'
require_relative './../../src/disk/disk_manager'
require_relative './../../src/buffer/buffer_pool'
require_relative './../../src/buffer/buffer_pool_manager'
require_relative './../../src/record/layout'
require_relative './../../src/record/schema'
require_relative './../../src/record/record_page'
require_relative './../../src/record/table_scan'
require_relative './../../src/query/select_scan'
require_relative './../../src/query/product_scan'
require_relative './../../src/query/project_scan'
require_relative './../../src/query/constant'
require_relative './../../src/query/expression'
require_relative './../../src/query/term'
require_relative './../../src/query/predicate'

class ProductScanTest < Minitest::Test
  def test_product_scan
    temp_file = Tempfile.open(mode: 32770)
    disk_manager = DiskManager.new(temp_file)
    buffer_pool = BufferPool.new(1)
    buffer_manager = BufferPoolManager.new(disk_manager, buffer_pool)

    schema = Schema.new
    schema.add_int_field('A')
    schema.add_string_field('B', 9)
    layout = Layout.new(schema)
    table_scan = TableScan.new(buffer_manager, 'T1', layout)

    # 今の実装では、テーブルごとにDiskManagerなどが必要になる
    temp_file2 = Tempfile.open(mode: 32770)
    disk_manager2 = DiskManager.new(temp_file2)
    buffer_pool2 = BufferPool.new(1)
    buffer_manager2 = BufferPoolManager.new(disk_manager2, buffer_pool2)

    schema2 = Schema.new
    schema2.add_int_field('C')
    schema2.add_string_field('D', 9)
    layout2 = Layout.new(schema2)
    table_scan2 = TableScan.new(buffer_manager2, 'T1', layout2)

    table_scan.before_first
    (0..1).each do |i|
      table_scan.insert

      table_scan.set_int('A', i)
      table_scan.set_string('B', "b_val#{i}")
    end

    table_scan2.before_first
    (0..1).each do |i|
      table_scan2.insert

      table_scan2.set_int('C', i)
      table_scan2.set_string('D', "d_val#{i}")
    end

    table_scan.before_first
    table_scan2.before_first

    product_scan = ProductScan.new(table_scan, table_scan2)

    actual = []
    loop do
      break unless product_scan.next

      actual.push("#{product_scan.get_int('A')}, #{product_scan.get_string('B')}, #{product_scan.get_int('C')}, #{product_scan.get_string('D')}")
    end

    # 直積の結果が取得できる
    expected = [
      '0, b_val0, 0, d_val0',
      '0, b_val0, 1, d_val1',
      '1, b_val1, 0, d_val0',
      '1, b_val1, 1, d_val1',
    ]

    assert_equal expected, actual
  end

  def test_table_select_project
    temp_file = Tempfile.open(mode: 32770)
    disk_manager = DiskManager.new(temp_file)
    buffer_pool = BufferPool.new(1)
    buffer_manager = BufferPoolManager.new(disk_manager, buffer_pool)

    schema = Schema.new
    schema.add_int_field('A')
    schema.add_string_field('B', 9)
    layout = Layout.new(schema)
    table_scan = TableScan.new(buffer_manager, 'T', layout)

    table_scan.before_first

    (0...200).each do |i|
      table_scan.insert
      table_scan.set_int('A', i / 10)
      table_scan.set_string('B', "rec#{i}")
    end

    table_scan2 = TableScan.new(buffer_manager, 'T', layout)

    l_expression = Expression.new(field_name: 'A')
    r_expression = Expression.new(value: Constant.new(10))

    term = Term.new(l_expression, r_expression)
    predicate = Predicate.new(term: term)
    assert_equal 'A=10', predicate.to_s

    select_scan = SelectScan.new(table_scan2, predicate)
    project_scan = ProjectScan.new(select_scan, 'B')

    actual = []
    loop do
      break unless project_scan.next

      actual.push(project_scan.get_string('B'))
    end

    # Aは i/10 なので、100-109の間が A=10 となる
    expected = %w[rec100 rec101 rec102 rec103 rec104 rec105 rec106 rec107 rec108 rec109]
    assert_equal expected, actual
  end

  def test_table_table_product_select_project
    temp_file = Tempfile.open(mode: 32770)
    disk_manager = DiskManager.new(temp_file)
    buffer_pool = BufferPool.new(1)
    buffer_manager = BufferPoolManager.new(disk_manager, buffer_pool)

    schema = Schema.new
    schema.add_int_field('A')
    schema.add_string_field('B', 9)
    layout = Layout.new(schema)
    table_scan = TableScan.new(buffer_manager, 'T1', layout)

    # 今の実装では、テーブルごとにDiskManagerなどが必要になる
    temp_file2 = Tempfile.open(mode: 32770)
    disk_manager2 = DiskManager.new(temp_file2)
    buffer_pool2 = BufferPool.new(1)
    buffer_manager2 = BufferPoolManager.new(disk_manager2, buffer_pool2)

    schema2 = Schema.new
    schema2.add_int_field('C')
    schema2.add_string_field('D', 9)
    layout2 = Layout.new(schema2)
    table_scan2 = TableScan.new(buffer_manager2, 'T1', layout2)

    table_scan.before_first
    (0...5).each do |i|
      table_scan.insert

      table_scan.set_int('A', i)
      table_scan.set_string('B', "b_val#{i}")
    end

    table_scan2.before_first
    (0...5).each do |i|
      table_scan2.insert

      table_scan2.set_int('C', i)
      table_scan2.set_string('D', "d_val#{i}")
    end

    table_scan1_p = TableScan.new(buffer_manager, 'T1', layout)
    table_scan2_p = TableScan.new(buffer_manager2, 'T2', layout2)

    product_scan = ProductScan.new(table_scan1_p, table_scan2_p)
    l_expression = Expression.new(field_name: 'A')
    r_expression = Expression.new(field_name: 'C')
    term = Term.new(l_expression, r_expression)
    predicate = Predicate.new(term: term)
    select_scan = SelectScan.new(product_scan, predicate)
    project_scan = ProjectScan.new(select_scan, %w[B D])

    actual = []
    loop do
      break unless project_scan.next

      actual.push("#{project_scan.get_string('B')}, #{project_scan.get_string('D')}")
    end

    expected = [
      'b_val0, d_val0',
      'b_val1, d_val1',
      'b_val2, d_val2',
      'b_val3, d_val3',
      'b_val4, d_val4',
    ]

    # AとCの値が等しいものだけ取得できる
    assert_equal expected, actual
  end
end
