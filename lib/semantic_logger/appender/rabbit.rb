# Gelf appender for rabbit

require 'uri'
begin
  require 'bunny'
  require 'oj'

  Oj.default_options = {:mode => :compat}
rescue => e
  puts e.message
  raise "Error in require part. Gems bunny and oj is required for running this appender"
end


class SemanticLogger::Appender::Rabbit < SemanticLogger::Subscriber

  # Map Semantic Logger levels to Graylog levels
  LEVEL_MAP = {
    fatal: GELF::FATAL,
    error: GELF::ERROR,
    warn: GELF::WARN,
    info: GELF::INFO,
    debug: GELF::DEBUG,
    trace: GELF::DEBUG
  }

  attr_reader :notifier

  def initialize(options = {}, &block)
    options = options.dup

    @rabbit_host=options.delete(:host)|| 'localhost'
    @port=options.delete(:port)|| 5672
    @vhost=options.delete(:vhost)||''
    @exchange=options.delete(:exchange)|| ''
    @user=options.delete(:user)||''
    @pw=options.delete(:pw)||''
    @app_name = options.delete(:application) || 'TUB'

    super(options, &block)
    self.application = @app_name

    reopen
  end

  def reopen
    @notifier = SemanticLogger::Notifier::RabbitNotifier.new(host: @rabbit_host, port: @port, vhost: @vhost, exchange_name: @exchange, user: @user, pw: @pw)
  end

  def make_hash(log)
    h = log.to_h(host, application)
    h[:level] = map_level(log)
    h[:level_str] = log.level.to_s
    h[:short_message] = h.delete(:message) if log.message && !h.key?("short_message") && !h.key?(:short_message)
    h[:request_uid] = h.delete(:tags).first if log.tags && log.tags.count > 0
    h
  end

  # Forward log messages
  def log(log)
    return false unless should_log?(log)

    @notifier.notify!(make_hash(log))
    true
  end

  # Returns the Graylog level for the supplied log message
  def map_level(log)
    LEVEL_MAP[log.level]
  end

end


module SemanticLogger
  module Notifier
    class RabbitNotifier

      attr_accessor :connection, :host, :port, :vhost, :exhange_name, :user, :pw, :default_options

      def initialize host:, port:, vhost:, exchange_name:, user:, pw:
        @connection = nil
        @host = host
        @port = port
        @vhost = vhost
        @exhange_name = exchange_name
        @user = user
        @pw = pw

        @default_options = {}
        @default_options['version'] = "1.1"
        @default_options['host'] ||= Socket.gethostname
        @default_options['level'] ||= GELF::UNKNOWN
        @default_options['facility'] ||= 'RabbitNotifier'

      end

      def connect
        unless @connection
          @connection = Bunny.new(:host => @host, :vhost => @vhost, :user => @user, :password => @pw)
          @connection.start
        end
        @connection
      end

      def gelfify(data)
        gdata = @default_options.dup
        data.keys.each do |key|
          value, key_s = data[key], key.to_s
          if ['host', 'level', 'version', 'short_message', 'full_message', 'timestamp', 'facility', 'line', 'file'].index(key_s)
            gdata[key_s] = value
          elsif key_s == 'action'
            gdata["_application_action"] = value
          elsif key_s == 'id'
            gdata["_application_id"] = value
          elsif key_s[0] != '_'
            gdata["_#{key_s}"] = value
          else
            gdata[key_s] = value
          end
        end
        gdata
      end

      def data_to_json(data)
        Oj.dump(gelfify(data))
      end

      def notify!(data)
        exchange.publish(data_to_json(data))
      end

      def channel
        connect
        @channel ||= @connection.create_channel
      end

      def exchange
        @exchange ||= channel.fanout(exhange_name, durable: true)
      end
    end
  end
end
