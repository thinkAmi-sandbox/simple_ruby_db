# frozen_string_literal: true

require 'simple_ruby_db/query/constant'

module SimpleRubyDb
  module Parse
    class Parser
      private attr_reader :lexer

      def initialize(value)
        @lexer = SimpleRubyDb::Parse::Lexer.new(value)
      end

      def field
        lexer.eat_identifier
      end

      def constant
        if lexer.match_string?
          SimpleRubyDb::Query::Constant.new(lexer.eat_string)
        else
          SimpleRubyDb::Query::Constant.new(lexer.eat_int)
        end
      end

      def expression
        if lexer.match_identifier?
          SimpleRubyDb::Query::Expression.new(field_name: field)
        else
          SimpleRubyDb::Query::Expression.new(value: constant)
        end
      end

      def term
        lhs = expression
        lexer.eat_delimiter('=')
        rhs = expression

        SimpleRubyDb::Query::Term.new(lhs, rhs)
      end

      def predicate
        pred = SimpleRubyDb::Query::Predicate.new(term: term)

        if lexer.match_keyword?('and')
          lexer.eat_keyword(keyword: 'and')
          pred.conjunction_with(predicate)
        end

        pred
      end

      def query
        lexer.eat_keyword(keyword: 'select')
        fields = select_list
        lexer.eat_keyword(keyword: 'from')
        tables = table_list

        pred = SimpleRubyDb::Query::Predicate.new
        if lexer.match_keyword?('where')
          lexer.eat_keyword(keyword: 'where')
          pred = predicate
        end

        SimpleRubyDb::Parse::QueryData.new(fields, tables, pred)
      end

      def select_list
        [].tap do |result|
          result.push(field)

          if lexer.match_delimiter?(',')
            lexer.eat_delimiter(',')
            result.concat(select_list)
          end
        end
      end

      def table_list
        [].tap do |result|
          result.push(lexer.eat_identifier)

          if lexer.match_delimiter?(',')
            lexer.eat_delimiter(',')
            result.concat(select_list)
          end
        end
      end

      def dml
        return insert_statement if lexer.match_keyword?('insert')
        return delete_statement if lexer.match_keyword?('delete')
        return update_statement if lexer.match_keyword?('update')
        create_statement
      end

      def create_statement
        lexer.eat_keyword(keyword: 'create')

        return create_table_statement if lexer.match_keyword?('table')
        return create_view_statement if lexer.match_keyword?('view')
        create_index_statement
      end

      def delete_statement
        lexer.eat_keyword(keyword: 'delete')
        lexer.eat_keyword(keyword: 'from')

        table_name = lexer.eat_identifier
        pred = SimpleRubyDb::Query::Predicate.new

        if lexer.match_keyword?('where')
          lexer.eat_keyword(keyword: 'where')
          pred = predicate
        end

        SimpleRubyDb::Parse::DeleteData.new(table_name, pred)
      end

      def update_statement
        lexer.eat_keyword(keyword: 'update')
        table_name = lexer.eat_identifier
        lexer.eat_keyword(keyword: 'set')
        f = field
        lexer.eat_delimiter('=')
        new_value = expression
        pred = SimpleRubyDb::Query::Predicate.new

        if lexer.match_keyword?('where')
          lexer.eat_keyword(keyword: 'where')
          pred = predicate
        end

        SimpleRubyDb::Parse::ModifyData.new(table_name, f, new_value, pred)
      end

      def insert_statement
        lexer.eat_keyword(keyword: 'insert')
        lexer.eat_keyword(keyword: 'into')
        table_name = lexer.eat_identifier
        lexer.eat_delimiter('(')
        f_list = field_list
        lexer.eat_delimiter(')')
        lexer.eat_keyword(keyword: 'values')
        lexer.eat_delimiter('(')
        c_list = constant_list
        lexer.eat_delimiter(')')

        SimpleRubyDb::Parse::InsertData.new(table_name, f_list, c_list)
      end

      private def field_list
        [].tap do |result|
          result.push(field)

          if lexer.match_delimiter?(',')
            lexer.eat_delimiter(',')
            result.concat(field_list)
          end
        end
      end

      private def constant_list
        [].tap do |result|
          result.push(constant)

          if lexer.match_delimiter?(',')
            lexer.eat_delimiter(',')
            result.concat(constant_list)
          end
        end
      end

      def create_table_statement
        lexer.eat_keyword(keyword: 'table')
        table_name = lexer.eat_identifier
        lexer.eat_delimiter('(')
        schema = field_definitions
        lexer.eat_delimiter(')')
        SimpleRubyDb::Parse::CreateTableData.new(table_name, schema)
      end

      private def field_definitions
        field_definition.tap do |schema|
          if lexer.match_delimiter?(',')
            lexer.eat_delimiter(',')
            schema.add_all(field_definitions)
          end
        end
      end

      private def field_definition
        field_name = field
        field_type(field_name)
      end

      private def field_type(field_name)
        SimpleRubyDb::Record::Schema.new.tap do |schema|
          if lexer.match_keyword?('int')
            lexer.eat_keyword(keyword: 'int')
            schema.add_int_field(field_name)
          else
            lexer.eat_keyword(keyword: 'varchar')
            lexer.eat_delimiter('(')
            string_length = lexer.eat_int
            lexer.eat_delimiter(')')
            schema.add_string_field(field_name, string_length)
          end
        end
      end

      def create_view_statement
        lexer.eat_keyword(keyword: 'view')
        view_name = lexer.eat_identifier
        lexer.eat_keyword(keyword: 'as')
        q = query
        SimpleRubyDb::Parse::CreateViewData.new(view_name, q)
      end

      def create_index_statement
        lexer.eat_keyword(keyword: 'index')
        index_name = lexer.eat_identifier
        lexer.eat_keyword(keyword: 'on')
        table_name = lexer.eat_identifier
        lexer.eat_delimiter('(')
        field_name = field
        lexer.eat_delimiter(')')
        SimpleRubyDb::Parse::CreateIndexData.new(index_name, table_name, field_name)
      end
    end
  end
end