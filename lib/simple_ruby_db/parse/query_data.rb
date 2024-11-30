# frozen_string_literal: true

module SimpleRubyDb
  module Parse
    class QueryData
      attr_reader :field_list
      attr_reader :table_list
      attr_reader :predicate

      def initialize(field_list, table_list, predicate)
        @field_list = field_list
        @table_list = table_list
        @predicate = predicate
      end

      def to_s
        result = 'select '

        field_list.each do |field|
          result += field + ', '
        end
        result = remove_final_comma(result)

        result += ' from '
        table_list.each do |table|
          result += table + ', '
        end
        result = remove_final_comma(result)

        pred_string = predicate.to_s
        return result if pred_string == ''

        "#{result} where #{pred_string}"
      end

      private def remove_final_comma(original_value)
        original_value[0, original_value.length - 2]
      end
    end
  end
end