require_relative './../parse/create_index_data'
require_relative './../parse/create_table_data'
require_relative './../parse/create_view_data'
require_relative './../parse/delete_data'
require_relative './../parse/insert_data'
require_relative './../parse/modify_data'
require_relative './../parse/query_data'
require_relative './../query/constant'
require_relative './../query/expression'
require_relative './../query/predicate'
require_relative './../query/term'
require_relative './../record/schema'
require_relative './lexer'

class Parser
  private attr_reader :lexer

  def initialize(value)
    @lexer = Lexer.new(value)
  end

  def field
    lexer.eat_identifier
  end

  def constant
    if lexer.match_string?
      Constant.new(lexer.eat_string)
    else
      x = lexer.eat_int
      Constant.new(x)
    end
  end

  def expression
    if lexer.match_identifier?
      Expression.new(field_name: field)
    else
      Expression.new(value: constant)
    end
  end

  def term
    lhs = expression
    lexer.eat_delimiter('=')
    rhs = expression

    Term.new(lhs, rhs)
  end

  def predicate
    pred = Predicate.new(term: term)

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

    pred = Predicate.new
    if lexer.match_keyword?('where')
      lexer.eat_keyword(keyword: 'where')
      pred = predicate
    end

    QueryData.new(fields, tables, pred)
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
    pred = Predicate.new

    if lexer.match_keyword?('where')
      lexer.eat_keyword(keyword: 'where')
      pred = predicate
    end

    DeleteData.new(table_name, pred)
  end

  def update_statement
    lexer.eat_keyword(keyword: 'update')
    table_name = lexer.eat_identifier
    lexer.eat_keyword(keyword: 'set')
    f = field
    lexer.eat_delimiter('=')
    new_value = expression
    pred = Predicate.new

    if lexer.match_keyword?('where')
      lexer.eat_keyword(keyword: 'where')
      pred = predicate
    end

    ModifyData.new(table_name, f, new_value, pred)
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

    InsertData.new(table_name, f_list, c_list)
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
    CreateTableData.new(table_name, schema)
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
    Schema.new.tap do |schema|
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
    CreateViewData.new(view_name, q)
  end

  def create_index_statement
    lexer.eat_keyword(keyword: 'index')
    index_name = lexer.eat_identifier
    lexer.eat_keyword(keyword: 'on')
    table_name = lexer.eat_identifier
    lexer.eat_delimiter('(')
    field_name = field
    lexer.eat_delimiter(')')
    CreateIndexData.new(index_name, table_name, field_name)
  end
end