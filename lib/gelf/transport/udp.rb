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

      def new_socket host, port
        socket = UDPSocket.new(Addrinfo.ip(host).afamily)
        socket.connect(host, port)
        socket
      end

      def create_consumer_for host, port
        Thread.new do
          retry_count = 0
          socket      = new_socket(host, port)

          loop do
            datagram = @q.pop
            break unless datagram

            begin
              retry_count += 1
              socket.send(datagram, 0)
              retry_count = 0

            rescue Errno::ECONNREFUSED
              $stderr.puts "#{Time.now}: Failed to send to GELF server. "\
                           "Resetting and trying again in #{retry_count}s."
              socket = new_socket(host, port)

              if retry_count < 5
                sleep retry_count
                retry
              else
                # we really want to propagate the error back up the
                # stack to a layer that has the full logical message;
                # that layer has the info needed to locally log the
                # full message...however, this requires a larger refactor
                $stderr.puts "#{Time.now}: Giving up on GELF for now."\
                             "Dropping log..."
              end
            end
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
