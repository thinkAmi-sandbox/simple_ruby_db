class Constant
  include Comparable

  attr_reader :int_value
  attr_reader :string_value

  def initialize(value)
    if integer_like?(value)
      # 文字列の数字も入ってくる可能性がある
      @int_value = value.to_i
      @string_value = nil
    else
      @int_value = nil
      # 文字列は 'foo' のようにシングルクォートで囲まれている場合もある
      # ただ、QueryData#to_s のときに文字列をシングルクォートで囲いたいことから、そのままにしておく
      @string_value = value
    end
  end

  private def integer_like?(value)
    Integer(value)
    true
  rescue ArgumentError
    false
  end

  # Javaの equals の代わりに定義
  def eql?(other)
    int_value.nil? ? string_value == other.string_value : int_value == other.int_value
  end

  # Javaの equals の代わりに定義
  def hash
    [int_value, string_value].hash
  end

  # Javaの compareTo の代わりに定義
  def <=>(other)
    int_value.nil? ? string_value <=> other.string_value : int_value <=> other.int_value
  end

  def to_s
    int_value.nil? ? string_value.to_s : int_value.to_s
  end
end