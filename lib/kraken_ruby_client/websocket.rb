module Kraken
  class WebSocket < Faye::WebSocket::Client
    class Error < StandardError; end

    def initialize(on_open: nil, on_close: nil)
      super "wss://ws.kraken.com", nil, ping: 180

      @handlers = OpenStruct.new(
        candlesticks: nil,
        pong: nil,
      )
      @request_id_inc = 0
      @user_stream_handlers = {}

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

    def candlesticks!(symbols, interval, &on_receive)
      symbols_fmt = symbols.is_a?(String) ? [symbols] : symbols
      @handlers.candlesticks = on_receive
      subscribe(symbols_fmt.map { |s| "#{s.downcase}@kline_#{interval}" })
    end

    def ping!(&on_pong)
      @handlers.pong = on_pong unless on_pong.nil?
      send({ event: :ping, reqid: request_id })
    end

    private

    def process_message!(data)
      json = JSON.parse(data, symbolize_names: true)
      if json[:error]
        raise Error.new("(#{json[:code]}) #{json[:msg]}")
      elsif json.key?(:result)
        #  Binance stream connected successfully
      else
        case json[:data][:e]&.to_sym
        when :kline
          @candlesticks_handler&.call(json[:stream], json[:data])
        when :outboundAccountPosition
        when :balanceUpdate
        when :executionReport # order update
          listen_key = json[:stream]
          @user_stream_handlers[listen_key]&.call(listen_key, json[:data])
        end
      end
    end

    def request_id
      @request_id_inc += 1
    end

    def subscribe(streams)
      send({
        method: "SUBSCRIBE",
        params: streams,
        id: request_id,
      }.to_json)
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
