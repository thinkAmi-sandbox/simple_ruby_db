# frozen_string_literal: true

module SimpleRubyDb
  module Plan
    class ProjectPlan
      attr_reader :plan
      attr_reader :schema

      def initialize(plan, field_list)
        @plan = plan

        @schema = SimpleRubyDb::Record::Schema.new.tap do |schema|
          field_list.each do |field_name|
            schema.add(field_name, plan.schema)
          end
        end
      end

      def open
        scan = plan.open
        SimpleRubyDb::Query::ProjectScan.new(scan, schema.fields)
      end

      def block_accessed
        plan.block_accessed
      end

      def records_output
        plan.records_output
      end

      def distinct_values(field_name)
        plan.distinct_values(field_name)
      end
    end
  end
end