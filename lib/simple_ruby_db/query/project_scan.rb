# frozen_string_literal: true

module SimpleRubyDb
  module Query
    class ProjectScan
      attr_reader :scan
      attr_reader :fields

      def initialize(scan, fields)
        @scan = scan
        @fields = fields
      end

      def before_first
        scan.before_first
      end

      def next
        scan.next
      end

      def get_int(field_name)
        return scan.get_int(field_name) if has_field?(field_name)

        raise "フィールド #{field_name} がありません"
      end

      def get_string(field_name)
        return scan.get_string(field_name) if has_field?(field_name)

        raise "フィールド #{field_name} がありません"
      end

      def has_field?(field_name)
        fields.include?(field_name)
      end

      def value(field_name)
        return scan.value(field_name) if has_field?(field_name)
        raise "フィールド #{field_name} が定義されていません"
      end
    end
  end
end