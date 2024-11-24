class Constant
  include Comparable

  attr_reader :int_value
  attr_reader :string_value

  def initialize(value)
    if value.is_a? Integer
      @int_value = value
      @string_value = nil
    else
      @int_value = nil
      @string_value = value
    end
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