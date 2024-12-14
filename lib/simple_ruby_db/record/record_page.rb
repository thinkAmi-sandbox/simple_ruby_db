# frozen_string_literal: true

module SimpleRubyDb
  module Record
    class RecordPage
      attr_reader :page_id
      attr_reader :layout
      attr_reader :buffer_pool_manager

      def initialize(buffer_pool_manager, page_id, layout)
        @buffer_pool_manager = buffer_pool_manager
        @page_id = page_id
        @layout = layout
      end

      def get_int(field_name)
        field_position = layout.offset(field_name)

        buffer = buffer_pool_manager.fetch_page(page_id)
        v = buffer.page[field_position...field_position + layout.integer_byte_length]
        v.pack('C*').unpack1('C*')
      end

      def get_string(field_name)
        field_position = layout.offset(field_name)
        field_length = layout.schema.field_length(field_name)

        buffer = buffer_pool_manager.fetch_page(page_id)
        v = buffer.page[field_position...field_position + field_length]

        # ゼロ埋めしてあると適切な文字にならないので、末尾の0を削除
        v.pop while v.last == 0
        v.pack('C*')
      end

      def set_int(field_name, value)
        field_position = layout.offset(field_name)

        buffer = buffer_pool_manager.fetch_page(page_id)
        buffer.page[field_position...field_position + layout.integer_byte_length] = [value].pack('C*').bytes
        buffer.is_dirty = true
      end

      def set_string(field_name, value)
        field_position = layout.offset(field_name)
        field_length = layout.schema.field_length(field_name)

        buffer = buffer_pool_manager.fetch_page(page_id)

        new_value = Array.new(field_length)
        new_value[0...value.bytes.size] = value.bytes

        # values.byteの長さが足りない場合全体が縮まってしまうので、不足分だけ末尾に要素 0 を追加する
        buffer.page[field_position...field_position + field_length] = new_value.map { |v| v.nil? ? 0 : v }
        buffer.is_dirty = true
      end

      def format
        layout.schema.fields.each do |field_name|
          if layout.schema.field_type(field_name) == 'integer'
            set_int(field_name, 0)
          else
            set_string(field_name, '')
          end
        end
      end

      # slot がないため、不要なメソッド
      # next_after
      # insert_after
      # set_flag
      # search_after
      # is_invalid_slot
      # offset
    end
  end
end