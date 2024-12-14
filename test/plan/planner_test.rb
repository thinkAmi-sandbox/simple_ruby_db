# frozen_string_literal: true

require_relative '../test_helper'

module Plan
  class PlannerTest < Minitest::Test
    def test_basic_query_planner
      temp_file = Tempfile.open(mode: 32770)
      temp_meta_file = Tempfile.open(mode: 32770)

      db = SimpleRubyDb::SimpleDb.new(temp_file, temp_meta_file, query_planner: 'basic')

      planner = db.planner

      # CREATE TABLE
      planner.execute_update('CREATE TABLE mytable(a int, b varchar(9))', db.metadata_buffer_pool_manager)

      # INSERT
      (0...200).each do |i|
        sql = "INSERT INTO mytable(a, b) VALUES(#{i / 10}, 'rec#{i}')"
        planner.execute_update(sql, db.buffer_pool_manager)
      end

      # SELECTで確認
      query = "SELECT a, b FROM mytable WHERE a = 10"
      plan = planner.create_query_plan(query, db.buffer_pool_manager)
      scan = plan.open

      actual_insert = [].tap do |result|
        while scan.next
          # デバッグ用コード
          # puts "a: #{scan.get_int('a')}, b: #{scan.get_string('b')}"
          result.push(scan.get_string('b'))
        end
      end

      # 各ヒープファイルの中身を確認する場合はこちらのコードをアンコメント
      # 中身はバイナリエディタ(VSCodeのHex Editorなど)で確認可能
      # copy_heap_file(temp_file)
      # copy_heap_file(temp_meta_file, is_meta: true)

      # 列aは i/10 なので、100-109の間が a=10 となる
      # 今回のテーブルの場合、シングルークォートも含んだ文字列を保存している
      expected_insert = %w['rec100' 'rec101' 'rec102' 'rec103' 'rec104' 'rec105' 'rec106' 'rec107' 'rec108' 'rec109']
      assert_equal expected_insert, actual_insert


      # UPDATE
      sql = "UPDATE mytable SET b = 'updated' WHERE a = 10"
      planner.execute_update(sql, db.buffer_pool_manager)

      # SELECTで確認
      query = "SELECT a, b FROM mytable WHERE a = 10"
      plan = planner.create_query_plan(query, db.buffer_pool_manager)
      scan = plan.open

      actual_update = [].tap do |result|
        while scan.next
          # デバッグ用コード
          # puts "a: #{scan.get_int('a')}, b: #{scan.get_string('b')}"
          result.push(scan.get_string('b'))
        end
      end

      expected_update = %w['updated' 'updated' 'updated' 'updated' 'updated' 'updated' 'updated' 'updated' 'updated' 'updated']
      assert_equal expected_update, actual_update


      # DELETE文は実装していないことを確認
      sql = "DELETE FROM mytable WHERE b = 'rec111'"
      error = assert_raises(RuntimeError) do
        planner.execute_update(sql, db.buffer_pool_manager)
      end

      assert_equal 'The DELETE statement is not implemented.', error.message
    end
  end

  def copy_heap_file(file, is_meta: false)
    base_name = is_meta ? 'meta' : 'data'
    destination_path = File.join(Dir.pwd, 'debug', base_name)

    FileUtils.mkdir_p(destination_path) unless Dir.exist?(destination_path)
    FileUtils.cp(file.path, destination_path)
  end
end
