require 'thread'

module GELF
  module Transport
    class UDP
      attr_reader :addresses

      def initialize(initial_addresses)
        @consumers = []
        @q = SizedQueue.new(128)
        self.addresses = initial_addresses
      end

      def addresses=(new_addresses)
        @addresses = new_addresses
        reset_sockets
      end

      def send_datagrams(datagrams)
        datagrams.each do |datagram|
          @q << datagram
        end
      end

      def close
        reset_sockets
      end

      private

      def create_consumer_for host, port
        Thread.new do
          socket = UDPSocket.new(Addrinfo.ip(host).afamily)
          socket.connect(host, port)

          loop do
            datagram = @q.pop
            break unless datagram
            socket.send(datagram, 0)
          end
        end
      end

      def reset_sockets
        @consumers.count.times do
          @q << nil
        end
        @consumers = @addresses.map do |address|
          create_consumer_for(*address)
        end
      end
    end
  end
end
