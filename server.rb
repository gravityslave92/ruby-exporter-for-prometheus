# Server is an async socket  connection handler
# he gathers connections to his server, which is unix socket
# and put them into queue. The he utilizes queue by shifting messages from the queue.
# A shifted message then goes to  collector by yielding to the handlers block
class Server
  require 'socket'
  require 'monitor'

  attr_reader :sockets, :sockets_available, :messages,
              :sockets_to_close, :can_close, :server

  def initialize
    @server = UNIXServer.new('sock')
    @messages = Queue.new

    @sockets = []
    @sockets_to_close = []

    @sockets.extend MonitorMixin
    @sockets_to_close.extend MonitorMixin

    @sockets_available = @sockets.new_cond
    @can_close = @sockets_to_close.new_cond
  end

  def on_message
    handle_connections_async
    collect_msgs_async
    close_sockets_async

    yield(messages.shift) while messages.any?
  end

  private

  def collect_msgs_async
    Thread.new do
      sockets.synchronize do
        sockets_available.wait

        watch_socket(sockets.shift) while sockets.any?
      end
    end
  end

  def watch_socket(socket)
    Thread.new(socket) do |sock|
      loop do
        msg = sock.gets
        next unless msg.empty?

        mark_socket_as_closed(sock)
        break
      end
    end
  end

  def mark_socket_as_closed(socket)
    sockets_to_close.synchronize do
      sockets_to_close << socket
      can_close.signal
    end
  end

  def handle_connections_async
    Thread.new do
      loop do
        socket = server.accept

        sockets.syncronize do
          sockets << socket
          sockets_available.signal
        end
      end
    end
  end

  def close_socket_async
    Thread.new do
      loop do
        sockets_to_close.synchronize do
          can_close.wait

          sockets_to_close.shift.close while sockets_to_close.any?
        end
      end
    end
  end
end

# exporter is a bridge between the server class and the prometheus
#  it register 2 metrics(a counter and a gauge) and prepares data
#  which will be scraped by prometheus
class Exporter
  require 'prometheus/client'
  require 'prometheus/client/formats/text'

  attr_reader :collector, :uniques, :active_users, :registry

  def initialize(collector:)
    @collector = collector
    @registry = Prometheus::Client.registry
    @uniques = Prometheus::Client::Counter.new(
      :unique_users_total,
      'Users total with unique id'
    )
    @active_users = Prometheus::Client::Gauge.new(
      :active_users,
      'Active users over time period'
    )
    @registry.register(@uniques)
    @registry.register(@active_users)
  end

  def prepare_data
    data = collector.export_data
    inc_by = (data[:uniques_count] - uniques.get)
    uniques.increment({}, inc_by)

    data[:active_users_gauge].each do |sec, counter|
      active_users.set({ segment_seconds: sec }, counter)
    end

    Prometheus::Client::Formats::Text.marshal(registry)
  end
end

# Collector is a data extractor from ruby server
# it depends on time periods and flush interval
# Collector stores users depending on current time period element
# and flushes the data depending on  the element
class Collector
  require 'set'
  require 'monitor'

  attr_reader :data, :lock, :uniques,
              :time_slices, :active_users

  def initialize(time_slices: [30, 60, 300], data_flush_interval: 5)
    setup_data(time_slices)
    setup_active_users(time_slices)

    @time_slices  = time_slices
    @uniques      = Set.new
    @lock         = Monitor.new
    @flusher      = Thread.new { handle_flush(data_flush_interval) }
  end

  def process(id)
    uniques << id
    lock.synchronize { data.each_value { |hash| hash[:users] << id } }
  end

  def export_data
    lock.synchronize do
      { unique_count: @uniques.count, active_users_gauge: @active_users }
    end
  end

  private

  def setup_data(time_slices)
    @data = time_slices.each_with_object({}) do |acc, sec|
      acc[sec.to_s] = { users: Set.new, next_flush_at: Time.now.to_i + sec }
      acc
    end
  end

  def setup_active_users(time_slices)
    @active_users = time_slices.each_with_object({}) do |acc, sec|
      acc.merge(sec.to_s => 0)
    end
  end

  def flush_data
    time = Time.now
    time_slices.each do |time_slice|
      next unless data[time_slice.to_s][:next_flush_at] <= time

      lock.synchronize { collect_data_and_flush(time_slice, time) }
    end
  end

  def collect_data_and_flush(time_slice, time)
    time_key = time_slice.to_s
    active_users[time_key] = data[time_key][:users].count

    data[time_key].tap do |hash|
      hash[:users].clear
      hash[:next_flush_at] = time + time_slice
    end
  end

  def handle_flush(interval)
    loop do
      sleep interval
      flush_data
    end
  end
end

# Handler is a wrapper fir collector, exporter and source server
# its purpose is to let prometheus scrap metrics he need
class Handler
  require 'webrick'

  attr_reader :exporter

  def initialize
    @source    = Server.new
    @collector = Collector.new
    @exporter  = Exporter.new(collector: @collector)
    Thread.new { @source.on_message { |msg| @collector.process(msg) } }
  end

  def run
    server = WEBrick::HTTPServer.new(Port: 8000)
    server.mount_proc '/metrics' do |_, res|
      res.body = exporter.prepare_data
    end

    server.start
  end
end
