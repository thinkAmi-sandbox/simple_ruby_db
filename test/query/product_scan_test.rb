# frozen_string_literal: true

require_relative '../test_helper'

module Query
  class ProductScanTest < Minitest::Test
    def test_product_scan
      temp_file = Tempfile.open(mode: 32770)
      disk_manager = SimpleRubyDb::Disk::DiskManager.new(temp_file)
      buffer_pool = SimpleRubyDb::Buffer::BufferPool.new(1)
      buffer_manager = SimpleRubyDb::Buffer::BufferPoolManager.new(disk_manager, buffer_pool)

      schema = SimpleRubyDb::Record::Schema.new
      schema.add_int_field('A')
      schema.add_string_field('B', 9)
      layout = SimpleRubyDb::Record::Layout.new(schema)
      table_scan = SimpleRubyDb::Record::TableScan.new(buffer_manager, 'T1', layout)

      # 今の実装では、テーブルごとにDiskManagerなどが必要になる
      temp_file2 = Tempfile.open(mode: 32770)
      disk_manager2 = SimpleRubyDb::Disk::DiskManager.new(temp_file2)
      buffer_pool2 = SimpleRubyDb::Buffer::BufferPool.new(1)
      buffer_manager2 = SimpleRubyDb::Buffer::BufferPoolManager.new(disk_manager2, buffer_pool2)

      schema2 = SimpleRubyDb::Record::Schema.new
      schema2.add_int_field('C')
      schema2.add_string_field('D', 9)
      layout2 = SimpleRubyDb::Record::Layout.new(schema2)
      table_scan2 = SimpleRubyDb::Record::TableScan.new(buffer_manager2, 'T1', layout2)

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

      product_scan = SimpleRubyDb::Query::ProductScan.new(table_scan, table_scan2)

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
      disk_manager = SimpleRubyDb::Disk::DiskManager.new(temp_file)
      buffer_pool = SimpleRubyDb::Buffer::BufferPool.new(1)
      buffer_manager = SimpleRubyDb::Buffer::BufferPoolManager.new(disk_manager, buffer_pool)

      schema = SimpleRubyDb::Record::Schema.new
      schema.add_int_field('A')
      schema.add_string_field('B', 9)
      layout = SimpleRubyDb::Record::Layout.new(schema)
      table_scan = SimpleRubyDb::Record::TableScan.new(buffer_manager, 'T', layout)

      table_scan.before_first

      (0...200).each do |i|
        table_scan.insert
        table_scan.set_int('A', i / 10)
        table_scan.set_string('B', "rec#{i}")
      end

      table_scan2 = SimpleRubyDb::Record::TableScan.new(buffer_manager, 'T', layout)

      l_expression = SimpleRubyDb::Query::Expression.new(field_name: 'A')
      r_expression = SimpleRubyDb::Query::Expression.new(value: SimpleRubyDb::Query::Constant.new(10))

      term = SimpleRubyDb::Query::Term.new(l_expression, r_expression)
      predicate = SimpleRubyDb::Query::Predicate.new(term: term)
      assert_equal 'A=10', predicate.to_s

      select_scan = SimpleRubyDb::Query::SelectScan.new(table_scan2, predicate)
      project_scan = SimpleRubyDb::Query::ProjectScan.new(select_scan, 'B')

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
      disk_manager = SimpleRubyDb::Disk::DiskManager.new(temp_file)
      buffer_pool = SimpleRubyDb::Buffer::BufferPool.new(1)
      buffer_manager = SimpleRubyDb::Buffer::BufferPoolManager.new(disk_manager, buffer_pool)

      schema = SimpleRubyDb::Record::Schema.new
      schema.add_int_field('A')
      schema.add_string_field('B', 9)
      layout = SimpleRubyDb::Record::Layout.new(schema)
      table_scan = SimpleRubyDb::Record::TableScan.new(buffer_manager, 'T1', layout)

      # 今の実装では、テーブルごとにDiskManagerなどが必要になる
      temp_file2 = Tempfile.open(mode: 32770)
      disk_manager2 = SimpleRubyDb::Disk::DiskManager.new(temp_file2)
      buffer_pool2 = SimpleRubyDb::Buffer::BufferPool.new(1)
      buffer_manager2 = SimpleRubyDb::Buffer::BufferPoolManager.new(disk_manager2, buffer_pool2)

      schema2 = SimpleRubyDb::Record::Schema.new
      schema2.add_int_field('C')
      schema2.add_string_field('D', 9)
      layout2 = SimpleRubyDb::Record::Layout.new(schema2)
      table_scan2 = SimpleRubyDb::Record::TableScan.new(buffer_manager2, 'T1', layout2)

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

      table_scan1_p = SimpleRubyDb::Record::TableScan.new(buffer_manager, 'T1', layout)
      table_scan2_p = SimpleRubyDb::Record::TableScan.new(buffer_manager2, 'T2', layout2)

      product_scan = SimpleRubyDb::Query::ProductScan.new(table_scan1_p, table_scan2_p)
      l_expression = SimpleRubyDb::Query::Expression.new(field_name: 'A')
      r_expression = SimpleRubyDb::Query::Expression.new(field_name: 'C')
      term = SimpleRubyDb::Query::Term.new(l_expression, r_expression)
      predicate = SimpleRubyDb::Query::Predicate.new(term: term)
      select_scan = SimpleRubyDb::Query::SelectScan.new(product_scan, predicate)
      project_scan = SimpleRubyDb::Query::ProjectScan.new(select_scan, %w[B D])

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
end
