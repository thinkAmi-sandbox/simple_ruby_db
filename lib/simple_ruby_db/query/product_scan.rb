# frozen_string_literal: true

module SimpleRubyDb
  module Query
    class ProductScan
      attr_reader :lhs_scan
      attr_reader :rhs_scan

      def initialize(lhs_scan, rhs_scan)
        @lhs_scan = lhs_scan
        @rhs_scan = rhs_scan

        before_first
      end

      def before_first
        lhs_scan.before_first
        lhs_scan.next

        rhs_scan.before_first
      end

      def next
        return true if rhs_scan.next

        rhs_scan.before_first
        rhs_scan.next && lhs_scan.next
      end

      def get_int(field_name)
        return lhs_scan.get_int(field_name) if lhs_scan.has_field?(field_name)

        rhs_scan.get_int(field_name)
      end

      def get_string(field_name)
        return lhs_scan.get_string(field_name) if lhs_scan.has_field?(field_name)

        rhs_scan.get_string(field_name)
      end

      def has_field?(field_name)
        lhs_scan.has_field?(field_name) || rhs_scan.has_field?(field_name)
      end

      def value(field_name)
        return lhs_scan.value(field_name) if lhs_scan.has_field?(field_name)
        rhs_scan.value(field_name)
      end
    end
  end
end