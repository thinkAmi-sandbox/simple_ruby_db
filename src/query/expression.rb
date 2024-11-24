class Expression
  attr_reader :value
  attr_reader :field_name

  def initialize(value: nil, field_name: nil)
    @value = value
    @field_name = field_name
  end

  def evaluate(scan)
    return value unless value.nil?
    scan.value(field_name)
  end

  def field_name?
    !field_name.nil?
  end

  def applies_to?(schema)
    return true unless value.nil?

    schema.has_field?(field_name)
  end

  def to_s
    return value.to_s unless value.nil?
    field_name.to_s
  end
end