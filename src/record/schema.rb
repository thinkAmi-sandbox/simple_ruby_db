class Schema
  attr_accessor :fields
  attr_accessor :field_info

  FIELD_TYPE = [:integer, :varchar]

  def initialize
    @fields = []
    @field_info = {}
  end

  def add_field(field_name, field_type, field_length)
    @fields.push(field_name)

    # field_infoを探しやすいよう、ハッシュのキーは文字列にしておく
    @field_info[field_name] = FieldInfo.new(field_type, field_length)
  end

  def add_int_field(field_name)
    add_field(field_name, :integer, 0)
  end

  def add_string_field(field_name, length)
    add_field(field_name, :varchar, length)
  end

  def add(field_name, schema)
    add_field(field_name, schema.field_type(field_name), schema.field_length(field_name))
  end

  def add_all(schema)
    schema.fields.each do |field_name|
      add(field_name, schema)
    end
  end

  def has_field?(field_name)
    fields.include?(field_name)
  end

  def field_type(field_name)
    field_info[field_name].field_type
  end

  def field_length(field_name)
    field_info[field_name].field_length
  end
end

# https://docs.ruby-lang.org/ja/latest/class/Data.html
FieldInfo = Data.define(:field_type, :field_length)