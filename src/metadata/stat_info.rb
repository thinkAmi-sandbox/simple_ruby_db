class StatInfo
  attr_reader :number_of_block
  attr_reader :number_of_records

  def initialize(number_of_block, number_of_records)
    @number_of_block = number_of_block
    @number_of_records = number_of_records
  end

  def block_accessed
    number_of_block
  end

  def records_output
    number_of_records
  end

  def distinct_values(_field_name)
    1 + (number_of_records / 3)
  end
end