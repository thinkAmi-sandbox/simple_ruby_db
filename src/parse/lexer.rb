require 'strscan'

class Lexer
  attr_reader :string_scanner

  KEYWORDS = [
    'select', 'from', 'where', 'and',
    'insert', 'into', 'values',
    'delete',
    'update', 'set',
    'create', 'table', 'int', 'varchar',
    'view', 'as',
    'index', 'on'
  ]

  # KEYWORDから正規表現の文字クラスを生成する
  # 各キーワードを `|` でつなぎ、\bで独立した単語にして、 /i で大文字・小文字を区別しない
  KEYWORDS_REGEX = /\b(?:#{KEYWORDS.join('|')})\b/i

  # 文字列のうち、KEYWORDとASCII文字、かつ、数字のみ以外でないときに、identifierとみなすための文字クラス
  # IDENTIFIER_REGEX = /\b(?!#{KEYWORDS.join('|')}\b)\w+\b/i

  # (?!\d+\b) 数字のみの単語ではない、を追加したい場合は以下を付かう
  IDENTIFIER_REGEX = /\b(?!#{KEYWORDS.join('|')}\b)(?!\d+\b)\w+\b/i

  def initialize(string_value)
    @string_scanner = StringScanner.new(string_value)
  end

  def eat_delimiter(delimiter)
    r = string_scanner.scan(/[#{delimiter}]/)
    raise 'シンタックスに誤りがあります' if r.nil?

    skip_space
    r
  end

  def match_delimiter?(delimiter)
    string_scanner.match?(/[#{delimiter}]/)
  end

  def eat_int
    r = string_scanner.scan(/[0-9]+/)
    raise 'シンタックスに誤りがあります' if r.nil?

    skip_space

    # 数値は int 型で返す必要がある
    r.to_i
  end

  def eat_string
    # 大文字・小文字区別せず、英文字を取得する
    # なお、stringなので、前後にシングルークォートがある
    r = string_scanner.scan(/'[[:word:]]+'/)
    raise 'シンタックスに誤りがあります' if r.nil?

    skip_space
    r
  end

  def match_string?
    # stringでマッチするか判定する際、前後にシングルクォートがある事も忘れない
    string_scanner.match?(/'[[:word:]]+'/i)
  end

  def eat_keyword(keyword: nil)
    word = keyword.nil? ? KEYWORDS_REGEX : /\b#{keyword}\b/i

    r = string_scanner.scan(word)
    raise 'シンタックスに誤りがあります' if r.nil?

    # キーワードにマッチしたときは、戻り値はないが、後続の空白はスキップしておく
    skip_space
  end

  def match_keyword?(keyword)
    string_scanner.match?(/#{keyword}/i)
  end

  def eat_identifier
    # 文字列の場合、クォートで囲まれている場合があるのでスキップする
    skip_quote

    # 大文字・小文字区別せず、英文字を取得する
    r = string_scanner.scan(IDENTIFIER_REGEX)
    raise 'シンタックスに誤りがあります' if r.nil?

    # 同様に、終わったあとにクォートが存在するならばスキップする
    skip_quote
    skip_space
    r
  end

  def match_identifier?
    string_scanner.match?(IDENTIFIER_REGEX)
  end

  def skip_space
    string_scanner.skip(/\s+/)
  end

  def skip_quote
    string_scanner.skip(/'/)
  end
end