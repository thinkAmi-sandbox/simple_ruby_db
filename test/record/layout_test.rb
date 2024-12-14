# frozen_string_literal: true

require_relative '../test_helper'

module Record
  class LayoutTest < Minitest::Test
    def test_layout
      s = SimpleRubyDb::Record::Schema.new

      s.add_int_field('int_field_name')
      s.add_string_field('string_field_name', 9)

      l = SimpleRubyDb::Record::Layout.new(s)

      actual = l.offset('int_field_name')
      # 使用済・未使用のフラグがないため、0始まり
      assert_equal 0, actual

      actual = l.offset('string_field_name')
      assert_equal 4, actual
    end
  end
end