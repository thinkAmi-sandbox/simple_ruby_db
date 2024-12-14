# frozen_string_literal: true

require 'simple_ruby_db/simple_db'
require 'simple_ruby_db/version'
require 'simple_ruby_db/buffer/buffer'
require 'simple_ruby_db/buffer/buffer'
require 'simple_ruby_db/buffer/buffer_pool'
require 'simple_ruby_db/buffer/buffer_pool_manager'
require 'simple_ruby_db/buffer/frame'
require 'simple_ruby_db/disk/disk_manager'
require 'simple_ruby_db/metadata/metadata_manager'
require 'simple_ruby_db/metadata/table_manager'
require 'simple_ruby_db/parse/create_index_data'
require 'simple_ruby_db/parse/create_table_data'
require 'simple_ruby_db/parse/create_view_data'
require 'simple_ruby_db/parse/delete_data'
require 'simple_ruby_db/parse/insert_data'
require 'simple_ruby_db/parse/lexer'
require 'simple_ruby_db/parse/modify_data'
require 'simple_ruby_db/parse/parser'
require 'simple_ruby_db/parse/predicate_parser'
require 'simple_ruby_db/parse/query_data'
require 'simple_ruby_db/plan/basic_query_planner'
require 'simple_ruby_db/plan/basic_update_planner'
require 'simple_ruby_db/plan/better_query_planner'
require 'simple_ruby_db/plan/plan_creatable'
require 'simple_ruby_db/plan/planner'
require 'simple_ruby_db/plan/product_plan'
require 'simple_ruby_db/plan/project_plan'
require 'simple_ruby_db/plan/select_plan'
require 'simple_ruby_db/plan/table_plan'
require 'simple_ruby_db/query/expression'
require 'simple_ruby_db/query/expression'
require 'simple_ruby_db/query/predicate'
require 'simple_ruby_db/query/product_scan'
require 'simple_ruby_db/query/project_scan'
require 'simple_ruby_db/query/select_scan'
require 'simple_ruby_db/query/term'
require 'simple_ruby_db/record/field_info'
require 'simple_ruby_db/record/layout'
require 'simple_ruby_db/record/record_page'
require 'simple_ruby_db/record/schema'
require 'simple_ruby_db/record/table_scan'

module SimpleRubyDb
  class Error < StandardError; end
  # Your code goes here...
end
