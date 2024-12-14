# frozen_string_literal: true

module SimpleRubyDb
  module Query
    class SelectScan
      attr_reader :scan
      attr_reader :predicate

      def initialize(scan, predicate)
        @scan = scan
        @predicate = predicate
      end

      def before_first
        scan.before_first
      end

      def next
        while scan.next
          if predicate.satisfied?(scan)
            return true
          end
        end
        false
      end

      def get_int(field_name)
        scan.get_int(field_name)
      end

      def get_string(field_name)
        scan.get_string(field_name)
      end

      def value(field_name)
        scan.value(field_name)
      end

      def has_field?(field_name)
        scan.has_field?(field_name)
      end

      def set_int(field_name, value)
        scan.set_int(field_name, value)
      end

      def set_string(field_name, value)
        scan.set_string(field_name, value)
      end

      def set_value(field_name, value)
        scan.set_value(field_name, value)
      end

      def delete
        scan.delete
      end

      def insert
        scan.insert
      end

      # 様子見系
      # get_rid
      # move_to_rid
    end
  end
end