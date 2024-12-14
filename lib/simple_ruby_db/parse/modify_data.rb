# frozen_string_literal: true

module SimpleRubyDb
  module Parse
    ModifyData = Data.define(:table_name, :field_name, :new_value, :predicate)
  end
end

