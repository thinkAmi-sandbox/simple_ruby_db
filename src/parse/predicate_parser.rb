class PredicateParser
  private attr_reader :lexer

  def initialize(lexer)
    @lexer = lexer
  end

  def field
    lexer.eat_identifier
  end

  def constant
    if lexer.match_string?
      lexer.eat_string
    else
      lexer.eat_int
    end
  end

  def expression
    if lexer.match_identifier?
      field
    else
      constant
    end
  end

  def term
    expression
    lexer.eat_delimiter
    expression
  end

  def predicate
    term

    if lexer.match_keyword?('and')
      lexer.eat_keyword(keyword: 'and')
      predicate
    end
  end
end