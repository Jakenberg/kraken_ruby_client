# Author: hello@jakenberg.io

module Kraken
  class WebSocket < Faye::WebSocket::Client
    BASE_MESSAGE = "[Kraken Websocket #{KrakenRubyClient::VERSION}]"

    class Error < StandardError
      def message(msg)
        super "#{BASE_MESSAGE} #{msg}"
      end
    end

    @@logger = Logger.new(STDOUT)

    class << self
      def log(severity, message)
        @@logger.log(severity, "#{BASE_MESSAGE} #{message}")
      end

      def heartbeat_event_handler
        proc do |_|
          # log(Logger::INFO, "received heartbeat")
        end
      end

      def pong_event_handler
        proc do |json|
          log(Logger::INFO, "pong received with reqid: #{json[:reqid]}")
        end
      end

      def subscription_status_event_handler
        proc do |json|
          pair = json[:pair]
          status = json[:status]&.to_sym
          case status
          when :subscribed, :unsubscribed
            log(Logger::INFO, "#{status} to #{json[:channelName]}@#{pair}")
          when :error
            log(Logger::ERROR, "(#{pair}) #{json[:errorMessage]}. Subscription info: #{json[:subscription].to_json}")
          else
            log(Logger::WARN, "received unexpected subscription status: #{status}")
          end
        end
      end

      def system_status_event_handler
        proc do |json|
          log(Logger::INFO, "server v#{json[:version]} status: #{json[:status]}")
        end
      end
    end

    def initialize(on_open: nil, on_close: nil)
      super "wss://ws.kraken.com", nil

      @channel_ids = {}
      @events = OpenStruct.new(
        heartbeat: self.class.heartbeat_event_handler,
        pong: self.class.pong_event_handler,
        public: OpenStruct.new(
          ticker: nil,
          ohlc: nil,
          trade: nil,
          spread: nil,
          book: nil,
        ),
        subscriptionStatus: self.class.subscription_status_event_handler,
        systemStatus: self.class.system_status_event_handler,
      ).freeze
      @request_id_inc = 0

      on :open do |event|
        on_open&.call(event)
      end

      on :message do |event|
        process_message!(event.data)
      end

      on :close do |event|
        on_close&.call(event)
      end
    end

    def ping(&on_pong)
      self.class.log(Logger::INFO, "sending ping")
      @events.pong = on_pong unless on_pong.nil?
      send_message({ event: :ping, reqid: request_id })
    end

    # @param interval: valid values (in minutes): 1|5|15|30|60|240|1440|10080|21600
    # @param channel_name: one of book|ohlc|openorders|owntrades|spread|ticker|trade|*
    # @param channel_args: available keys: depth|ratecounter|snapshot|token
    #
    # more info: https://docs.kraken.com/websockets/#message-subscribe
    def subscribe(pairs, interval, channel_name, channel_args = {}, &on_receive)
      pairs = pairs.is_a?(String) ? [pairs] : pairs
      raise WebSocket::Error.new("channel name \"#{channel_name}\" is forbidden!") unless channel_name.nil? || public_event_keys.include?(channel_name.to_s)
      @events.public.send("#{channel_name}=", on_receive) unless on_receive.nil?
      send_message({
        event: :subscribe,
        pair: pairs,
        subscription: {
          name: channel_name,
          interval: interval,
        }.merge(channel_args),
      })
    end

    private

    def event_keys
      keys = []
      @events.each_pair { |k, _| keys << k.to_s }
      keys
    end

    def process_general_message(json)
      event_name = json[:event]
      if event_keys.include?(event_name)
        @events.send(event_name)&.call(json)
      else
        self.class.log(Logger::WARN, "received unknown message: #{event_name}")
      end
    end

    def process_message!(data)
      json = JSON.parse(data, symbolize_names: true)
      if json.is_a?(Array)
        process_public_message(json)
      else
        process_general_message(json)
      end
    end

    def process_public_message(json)
      channel_id, ticker, channel_full_name, pair = json
      @channel_ids[pair] = channel_id
      channel_name, interval = channel_full_name.split("-")
      if public_event_keys.include?(channel_name)
        @events.public.send(channel_name)&.call(pair, interval, ticker)
      else
        self.class.log(Logger::WARN, "received unknown message for #{pair}: #{channel_name}")
      end
    end

    def public_event_keys
      %w(ticker ohlc trade spread book).freeze
    end

    def request_id
      @request_id_inc += 1
    end

    def send_message(data)
      message = if data.respond_to?(:to_json)
          data.to_json
        else
          data
        end
      event_name = data.is_a?(Hash) ? data[:event] : "n/a"
      if send(message)
        self.class.log(Logger::INFO, "message sent for event: #{event_name}")
      else
        self.class.log(Logger::ERROR, "message data failed to send for event: #{event_name}")
      end
    end

    # Terminating socket connection achieves the same result.
    # If you have a use-case for this, please create a GitHub issue.
    #
    # def unsubscribe(streams)
    #   send({
    #     method: "UNSUBSCRIBE",
    #     params: streams,
    #     id: request_id,
    #   }.to_json)
    # end
  end
end
