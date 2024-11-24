class Layout
  attr_accessor :schema
  attr_accessor :offsets

  def initialize(schema, offsets: nil)
    @schema = schema

    unless offsets.nil?
      # メタデータの場合は offsets が渡されてくるので、それを利用する
      @offsets = offsets
      return
    end

    # slotは使わないので、使用済/未使用判定フラグ分の領域は不要
    @offsets = {}.tap do |o|
      position = 0

      schema.fields.each do |field_name|
        o[field_name] = position
        position += length_in_bytes(field_name)
      end
    end
  end

  def offset(field_name)
    offsets[field_name]
  end

  def length_in_bytes(field_name)
    field_type = schema.field_type(field_name)

    return integer_byte_length if field_type == :integer

    field_length = schema.field_length(field_name)
    string_byte_length(field_length)
  end

  def integer_byte_length
    [0].pack('i').bytesize
  end

  def string_byte_length(field_length)
    # 1文字あたりのバイト数を取得する方法がないので、固定値を返す
    bytes_per_char = 1

    integer_byte_length + (field_length * bytes_per_char)
  end
end