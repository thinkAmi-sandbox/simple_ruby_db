# frozen_string_literal: true

require_relative '../test_helper'

module Parse
  class LexerTest < Minitest::Test
    def test_sql
      sql = "SELECT a, b FROM mytable WHERE a = 1 AND b = 'foo'"

      lexer = SimpleRubyDb::Parse::Lexer.new(sql)
      lexer.eat_keyword(keyword: 'select')

      actual = lexer.eat_identifier
      assert_equal 'a', actual

      actual = lexer.eat_delimiter(',')
      assert_equal ',', actual

      actual = lexer.eat_identifier
      assert_equal 'b', actual

      lexer.eat_keyword(keyword: 'from')

      actual = lexer.eat_identifier
      assert_equal 'mytable', actual

      lexer.eat_keyword(keyword: 'where')

      actual = lexer.eat_identifier
      assert_equal 'a', actual

      actual = lexer.eat_delimiter('=')
      assert_equal '=', actual

      actual = lexer.eat_int
      assert_equal 1, actual

      lexer.eat_keyword(keyword: 'and')

      actual = lexer.eat_identifier
      assert_equal 'b', actual

      actual = lexer.eat_delimiter('=')
      assert_equal '=', actual

      actual = lexer.eat_string
      assert_equal "'foo'", actual
    end
  end
end
