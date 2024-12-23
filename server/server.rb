# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)

require 'socket'
require 'tempfile'
require 'simple_ruby_db'

class Server
  LENGTH_OF_MESSAGE_CONTENTS = 4
  OID = 0

  private attr_reader :port, :db, :planner

  def initialize(port=25432)
    @port = port

    data_file = "#{Dir.pwd}/datafile"
    meta_file = "#{Dir.pwd}/metafile"

    File.open(data_file, 'wb') { |f| } unless File.exist?(data_file)
    File.open(meta_file, 'wb') { |f| } unless File.exist?(meta_file)
    @db = SimpleRubyDb::SimpleDb.new(data_file, meta_file)
    @planner = @db.planner
  end

  def start
    puts "Start server on port #{port}"

    Socket.tcp_server_loop(port) do |sock, addr_info|
      startup_phase(sock)

      loop do
        break unless simple_query_phase(sock)
      end

    ensure
      sock.close

      puts 'End'
    end
  end

  private

  # ===========================================
  # 変換系メソッド
  # ===========================================
  def sb(value)
    # 文字列をバイナリ文字列にする
    [value].pack('A*')
  end

  def sbn(value)
    # 文字列の末尾にNULL終端文字列を追加してバイナリ文字列にする
    [value].pack('Z*')
  end

  def i32b(value)
    # ビッグエンディアン、32ビット符号つき整数としてバイナリ文字列にする
    [value].pack('l>')
  end

  def i16b(value)
    # ビッグエンディアン、16ビット符号つき整数としてバイナリ文字列にする
    [value].pack('s>')
  end

  def b32i(value)
    # バイナリ文字列を 32ビット符号つき整数 にする
    value.unpack1('l>')
  end

  # ===========================================
  # Start-upフェーズのメソッド
  # ===========================================
  def startup_phase(sock)
    ssl_request(sock)
    startup_message(sock)
    send_authentication_ok(sock)
    send_ready_for_query(sock)
  end

  def ssl_request(sock)
    # SSL Request で受け取った値は捨てて良い
    # 厳密には検証したほうが良さそう
    sock.recvmsg

    # SSL接続が不可なので、 'N' を返す
    sock.write 'N'
  end

  def startup_message(sock)
    # Startup Message では、クライアントからパケットが送られてくるが、すべて捨てて良い
    sock.recvmsg
  end

  def send_authentication_ok(sock)
    # 認証要求メッセージを送るが、パスワード設定は無視するため、OKでよい
    # https://www.postgresql.jp/document/16/html/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-AUTHENTICATIONOK
    msg = sb('R') + i32b(8) + i32b(0)
    sock.write msg
  end

  def send_ready_for_query(sock)
    # https://www.postgresql.jp/document/16/html/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-READYFORQUERY
    sock.write sb('Z') + i32b(5) + sb('I')
  end

  # ===========================================
  # Simple Queryフェーズのメソッド
  # ===========================================
  def simple_query_phase(sock)
    tag = receive_tag(sock)

    # 簡易問い合わせでない場合、処理を終了する
    # なお、 sock.recv したときには数値になっているので、文字コード変換が必要
    return false if tag.chr != 'Q'

    # メッセージ内容を取得する
    sql = receive_message_contents(sock)

    case sql
    in String if sql.start_with?('create table')
      send_command_complete_of_create_table(sock, sql)
      send_ready_for_query(sock)
    in String if sql.start_with?('insert')
      send_command_complete_of_insert(sock, sql)
      send_ready_for_query(sock)
    in String if sql.start_with?('update')
      send_command_complete_of_update(sock, sql)
      send_ready_for_query(sock)
    in String if sql.start_with?('select')
      send_row_description(sock, sql)
      selected_rows = send_data_row(sock, sql)
      send_command_complete_of_select(sock, selected_rows)
      send_ready_for_query(sock)
    else
      false
    end
  end

  def receive_tag(sock)
    # Socket#readbyteで1バイトだけ読み込む
    # https://docs.ruby-lang.org/ja/latest/method/IO/i/readbyte.html
    sock.readbyte
  end

  def receive_message_contents(sock)
    length = sock.read LENGTH_OF_MESSAGE_CONTENTS

    # lengthには、メッセージ内容の長さ + バイト単位の長さ が設定されている
    # すでにバイト単位の長さの情報は recv しているので、それ以外の長さをメッセージ内容の長さと考えて recv する
    # なお、length はバイナリ文字列なので、32bit整数へと変換してから演算する
    sql = sock.read(b32i(length) - LENGTH_OF_MESSAGE_CONTENTS)

    # SQL文字列を受信した想定だが、末尾に NULL終端文字列 が入っている可能性がある
    # そこで、 unpack('A*') で NULL終端文字列 を削除した後、再度 pack('A*') して、SQL文字列を取得している
    # また、SQLに大文字・小文字が混在していると取り扱いが手間かもしれないため、小文字だけにしておく
    sql.unpack('A*').pack('A*').downcase
  end

  ## CREATE TABLE向け
  def send_command_complete_of_create_table(sock, sql)
    planner.execute_update(sql, db.metadata_buffer_pool_manager)

    value = 'CREATE TABLE'
    value_length = value.bytesize + 1 # NULL終端文字列の分も長さとして計算する

    # CommandCompleteのStringタグにはNULL終端文字列が必要
    msg = sb('C') + i32b(value_length + LENGTH_OF_MESSAGE_CONTENTS) + sbn(value)
    sock.write msg
  end

  ## INSERT向け
  def send_command_complete_of_insert(sock, sql)
    planner.execute_update(sql, db.buffer_pool_manager)

    # 今回のRDBMSは1行ずつしかINSERTできないので、行数は1で固定
    value = "INSERT #{OID} 1"

    value_length = value.bytesize + 1 # NULL終端文字列の分も長さとして計算する

    # CREATE TABLE同様
    msg = sb('C') + i32b(value_length + LENGTH_OF_MESSAGE_CONTENTS) + sbn(value)
    sock.write msg
  end

  ## SELECT向け
  ### RowDescriptionメッセージ用
  def send_row_description(sock, sql)
    schema = db.metadata_manager.layout(table_name(sql)).schema
    request_field_list = field_list(sql)
    defs = request_field_list.each_with_index.map { |col_name, col_index| row_definition(schema, col_name, col_index) }

    contents = defs.reduce { |result, item| result + item }

    # length_of_message_contentsはInt32なので4バイト、number_of_columnはInt16なので2バイト
    # 上記2つに各列の内容を合わせたものが、メッセージ全体の長さ
    bytesize_for_length_of_message_contents = 4
    bytesize_for_number_of_column = 2

    message_bytesize = bytesize_for_length_of_message_contents + bytesize_for_number_of_column + contents.length

    # SELECT句で指定された列数を設定
    number_of_field_list = request_field_list.length

    msg = sb('T') + i32b(message_bytesize) + i16b(number_of_field_list) + contents
    sock.write(msg)
  end

  def table_name(sql)
    # 今回、テーブル名は1つしか指定できない仕様なので、 first で取得して問題ない
    SimpleRubyDb::Parse::Parser.new(sql).query.table_list.first
  end

  def field_list(sql)
    SimpleRubyDb::Parse::Parser.new(sql).query.field_list
  end

  def row_definition(schema, col_name, col_index)
    field_name = sbn(col_name)  # field name には NULL終端文字列 が必要
    object_id_of_table = 16385 # 適当な値
    column_id = col_index + 1

    # schema.field_type と schema.field_length を使って、列の型と列の長さを取得できる
    col_type = schema.field_type(col_name)
    is_integer = col_type == 'integer'

    # https://www.postgresql.jp/document/16/html/catalog-pg-type.html
    pg_type_oid = is_integer ? i32b(23) : i32b(1043)

    pg_type_typlen = is_integer ? i16b(4) : i16b(-1)

    # https://www.postgresql.jp/document/16/html/catalog-pg-attribute.html
    col_length = schema.field_length(col_name)
    pg_attribute_attypmod = is_integer ? i32b(-1) : i32b(col_length + 4)
    format_code = i16b(0)

    field_name + i32b(object_id_of_table) + i16b(column_id) + pg_type_oid + pg_type_typlen + pg_attribute_attypmod + format_code
  end

  ### DataRowメッセージ用
  def send_data_row(sock, sql)
    scan = db.planner.create_query_plan(sql, db.buffer_pool_manager).open

    schema = db.metadata_manager.layout(table_name(sql)).schema
    request_field_list = field_list(sql)
    col_defs = request_field_list.map { |field_name| {name: field_name, type: schema.field_type(field_name)} }

    # SELECT句で指定された列数を設定
    number_of_field_list = request_field_list.length

    row_values = [].tap do |result|
      while scan.next
        result.push(data_row(col_values(col_defs, scan), number_of_field_list))
      end
    end

    selected_rows = row_values.length

    message = row_values.reduce { |result, row| result + row }
    sock.write message

    selected_rows
  end

  def data_row(col_values, number_of_field_list)
    hex_values = col_values.map do |col_value|
      data_column(col_value)
    end

    contents = hex_values.reduce { |result, value| result + value }

    # length_of_message_contentsはInt32なので4バイト、number_of_columnはInt16なので2バイト
    # 上記2つに各列の内容を合わせたものが、メッセージ全体の長さ
    bytesize_for_length_of_message_contents = 4
    bytesize_for_number_of_column = 2
    message_bytesize = bytesize_for_length_of_message_contents + bytesize_for_number_of_column + contents.length

    sb('D') + i32b(message_bytesize) + i16b(number_of_field_list) + contents
  end

  def data_column(column_value)
    i32b(column_value.to_s.length) + sb(column_value.to_s)
  end

  def col_values(col_defs, scan)
    col_defs.map do |col_def|
      col_def[:type] == 'integer' ? scan.get_int(col_def[:name]) : scan.get_string(col_def[:name]).delete_prefix("'").delete_suffix("'")
    end
  end

  ## CommandComplete用
  def send_command_complete_of_select(sock, selected_rows)
    value = "SELECT #{selected_rows}"

    value_length = value.bytesize + 1 # NULL終端文字列の分も長さとして計算する

    # CREATE TABLE同様
    msg = sb('C') + i32b(value_length + LENGTH_OF_MESSAGE_CONTENTS) + sbn(value)
    sock.write msg
  end

  ## UPDATE向け
  def send_command_complete_of_update(sock, sql)
    number_of_updated_rows = planner.execute_update(sql, db.buffer_pool_manager)

    # 今回のRDBMSは1行ずつしかINSERTできないので、行数は1で固定
    value = "UPDATE #{number_of_updated_rows}"

    value_length = value.bytesize + 1 # NULL終端文字列の分も長さとして計算する

    # CREATE TABLE同様
    msg = sb('C') + i32b(value_length + LENGTH_OF_MESSAGE_CONTENTS) + sbn(value)
    sock.write msg
  end
end

server = Server.new
server.start