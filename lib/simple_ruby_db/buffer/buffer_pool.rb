# frozen_string_literal: true

module SimpleRubyDb
  module Buffer
    class BufferPool
      attr_accessor :buffers
      attr_accessor :next_victim_id

      def initialize(pool_size)
        @buffers = Array.new(pool_size)
        @buffers = @buffers.map { |v| v.nil? ? Frame.create : v }

        @next_victim_id = 0
      end

      def size
        @buffers.size
      end

      def [](index)
        @buffers[index]
      end

      def evict
        pool_size = self.size
        consecutive_pinned = 0

        loop do
          next_victim_id = @next_victim_id
          frame = self[next_victim_id]

          if frame.usage_count == 0
            return @next_victim_id
          end

          if frame.buffer.nil?
            consecutive_pinned += 1

            if consecutive_pinned >= pool_size
              return
            end
          else
            frame.usage_count -= 1
            consecutive_pinned = 0
          end

          @next_victim_id = increment_id(@next_victim_id)
        end
      end

      def increment_id(buffer_id)
        (buffer_id + 1) % self.size
      end
    end
  end
end

