require 'simple_ruby_db'

temp_file = Tempfile.open(mode: 32770)
temp_meta_file = Tempfile.open(mode: 32770)

db = SimpleRubyDb::SimpleDb.new(temp_file, temp_meta_file, query_planner: 'basic')
planner = db.planner


# CREATE TABLE
planner.execute_update('CREATE TABLE mytable(a int, b varchar(9))', db.metadata_buffer_pool_manager)


# INSERT
(0...200).each do |i|
  sql = "INSERT INTO mytable(a, b) VALUES(#{i / 10}, 'rec#{i}')"
  planner.execute_update(sql, db.buffer_pool_manager)
end

# SELECT
query = "SELECT a, b FROM mytable WHERE a = 10"
plan = planner.create_query_plan(query, db.buffer_pool_manager)
scan = plan.open

while scan.next
  puts "a: #{scan.get_int('a')}, b: #{scan.get_string('b')}"
end

sql = "UPDATE mytable SET b = 'updated' WHERE a = 10"
planner.execute_update(sql, db.buffer_pool_manager)

# SELECT
query = "SELECT a, b FROM mytable WHERE a = 10"
plan = planner.create_query_plan(query, db.buffer_pool_manager)
scan = plan.open

while scan.next
  puts "a: #{scan.get_int('a')}, b: #{scan.get_string('b')}"
end


# Check file content
# VSCode's Hex Editor is useful
destination_path = File.join(Dir.pwd, 'debug')

FileUtils.mkdir_p(destination_path) unless Dir.exist?(destination_path)
FileUtils.cp(temp_file.path, destination_path)