# frozen_string_literal: true

require_relative '../test_helper'

module Parse
  class ParserTest < Minitest::Test
    def test_query
      sql = "SELECT a, b FROM mytable WHERE a = 1 AND b = 'foo'"

      parser = SimpleRubyDb::Parse::Parser.new(sql)
      actual = parser.query

      assert_equal %w[a b], actual.field_list
      assert_equal ['mytable'], actual.table_list
      assert_equal "select a, b from mytable where a=1 and b='foo'", actual.to_s
    end

    def test_delete_statement
      sql = "DELETE FROM mytable WHERE a = 'foo'"

      parser = SimpleRubyDb::Parse::Parser.new(sql)
      actual = parser.delete_statement

      assert_equal 'mytable', actual.table_name

      expected = SimpleRubyDb::Parse::DeleteData.new(
        'mytable',
        SimpleRubyDb::Query::Predicate.new(
          term: SimpleRubyDb::Query::Term.new(
            SimpleRubyDb::Query::Expression.new(field_name: 'a'),
            SimpleRubyDb::Query::Expression.new(value: SimpleRubyDb::Query::Constant.new("'foo'")))))

      # オブジェクトは異なるが内容が同じかどうかを検証するため、 assert + inspect を使う
      assert expected.inspect, actual.inspect
    end

    def test_update_statement
      # UPDATEは1項目のみ対応
      sql = "UPDATE mytable SET a = 1 WHERE b = 'foo'"

      parser = SimpleRubyDb::Parse::Parser.new(sql)
      actual = parser.update_statement

      assert_equal 'mytable', actual.table_name

      expected = SimpleRubyDb::Parse::ModifyData.new(
        'mytable',
        'a',
        SimpleRubyDb::Query::Expression.new(value: SimpleRubyDb::Query::Constant.new(1)),
        SimpleRubyDb::Query::Predicate.new(term: SimpleRubyDb::Query::Term.new(
          SimpleRubyDb::Query::Expression.new(field_name: 'b'),
          SimpleRubyDb::Query::Expression.new(value: SimpleRubyDb::Query::Constant.new("'foo'")))))

      # オブジェクトは異なるが内容が同じかどうかを検証するため、 assert + inspect を使う
      assert expected.inspect, actual.inspect
    end

    def test_insert_statement
      sql = "INSERT INTO mytable (a, b) VALUES (1, 'foo')"

      parser = SimpleRubyDb::Parse::Parser.new(sql)
      actual = parser.insert_statement

      assert_equal 'mytable', actual.table_name

      expected = SimpleRubyDb::Parse::InsertData.new('mytable',
                                                     ['a', 'b'],
                                                     [
                                                       SimpleRubyDb::Query::Constant.new(1),
                                                       SimpleRubyDb::Query::Constant.new("'foo'"),
                                                     ])

      assert_equal expected, actual
    end

    def test_create_table_statement
      sql = 'CREATE TABLE mytable (a INT, b VARCHAR(10))'

      parser = SimpleRubyDb::Parse::Parser.new(sql)
      actual = parser.create_statement

      expected_schema = SimpleRubyDb::Record::Schema.new
      expected_schema.add_field('a', :integer, 0)
      expected_schema.add_field('b', :varchar , 10)

      expected = SimpleRubyDb::Parse::CreateTableData.new('mytable', expected_schema)

      # オブジェクトは異なるが内容が同じかどうかを検証するため、 assert + inspect を使う
      assert expected.inspect, actual.inspect
    end

    def test_create_view_statement
      sql = "CREATE VIEW myview AS SELECT a, b FROM mytable WHERE a = 1 AND b = 'foo'"

      parser = SimpleRubyDb::Parse::Parser.new(sql)
      actual = parser.create_statement

      expected_predicate = SimpleRubyDb::Query::Predicate.new
      expected_predicate.terms = [
        SimpleRubyDb::Query::Term.new(
          SimpleRubyDb::Query::Expression.new(field_name: 'a'),
          SimpleRubyDb::Query::Expression.new(value: SimpleRubyDb::Query::Constant.new(1))),
        SimpleRubyDb::Query::Term.new(
          SimpleRubyDb::Query::Expression.new(field_name: 'b'),
          SimpleRubyDb::Query::Expression.new(value: SimpleRubyDb::Query::Constant.new("'foo'"))),
      ]

      expected = SimpleRubyDb::Parse::CreateViewData.new(
        'myview',
        SimpleRubyDb::Parse::QueryData.new(['a', 'b'], ['mytable'], expected_predicate))

      # オブジェクトは異なるが内容が同じかどうかを検証するため、 assert + inspect を使う
      assert expected.inspect, actual.inspect
    end

    def test_create_index_statement
      sql = "CREATE INDEX myindex ON mytable (a)"

      parser = SimpleRubyDb::Parse::Parser.new(sql)
      actual = parser.create_statement

      expected = SimpleRubyDb::Parse::CreateIndexData.new('myindex', 'mytable', 'a')

      assert_equal expected, actual
    end
  end
end
