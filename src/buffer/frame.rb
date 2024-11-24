require_relative './buffer'

class Frame
  attr_accessor :usage_count
  attr_accessor :buffer

  def initialize(usage_count, buffer)
    @usage_count = usage_count
    @buffer = buffer
  end

  def self.create
    new(0, Buffer.create)
  end
end