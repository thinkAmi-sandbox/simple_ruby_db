# require_relative './page_id'
require 'fileutils'

# PageIdはクラスとして作成しない(単なるint型として扱う)
class DiskManager
  attr_accessor :heap_file
  attr_accessor :next_page_id

  PAGE_SIZE = 4096

  def initialize(heap_file)
    @heap_file = heap_file
    heap_file_size = heap_file.size
    @next_page_id = heap_file_size / PAGE_SIZE
  end

  def self.open(heap_file_path)
    # ファクトリメソッドとしての open を用意することで、テストコードで Tempfile が使いやすくなっている
    directory_path = File.dirname(heap_file_path)
    FileUtils.mkdir_p(directory_path) unless Dir.exist?(directory_path)

    f = File.open(heap_file_path, 'rb+')
    new(f)
  end

  def read_page_data(page_id)
    offset = PAGE_SIZE * page_id

    @heap_file.seek(offset)
    f = @heap_file.read(PAGE_SIZE)

    # バイト配列表現にする
    f.bytes
  end

  def write_page_data(page_id, data)
    offset = PAGE_SIZE * page_id
    @heap_file.seek(offset)

    # バイナリ形式で、"C*" を使って、各要素を4バイトとして書き込む
    @heap_file.write(data.pack('C*'))
  end

  def allocate_page
    page_id = @next_page_id
    @next_page_id = @next_page_id + 1

    page_id
  end

  def sync
    # バッファ内のデータをディスクに送る
    @heap_file.flush
    # データとファイルのメタデータをディスクに完全同期する
    @heap_file.fsync
  end
end