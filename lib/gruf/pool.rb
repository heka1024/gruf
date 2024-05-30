# frozen_string_literal: true

module Gruf
  class Pool < ::GRPC::Pool
    def initialize(size, keep_alive: DEFAULT_KEEP_ALIVE)
      super(size, keep_alive: keep_alive)
    end

    # Runs the given block on the queue with the provided args.
    #
    # @param args the args passed blk when it is called
    # @param blk the block to call
    def schedule(*args, &blk)
      return if blk.nil?
      @stop_mutex.synchronize do
        if @stopped
          GRPC.logger.warn("did not schedule job, already stopped")
          return
        end
        GRPC.logger.info("schedule another job")

        @jobs << [blk, args]
      end
    end

    # Starts running the jobs in the thread pool.
    def start
      @stop_mutex.synchronize do
        fail "already stopped" if @stopped
      end
      until @workers.size == @size.to_i
        # new_worker_queue = Queue.new
        # @ready_workers << new_worker_queue
        next_thread = Thread.new do
          catch(:exit) do
            # allows { throw :exit } to kill a thread
            _loop_execute_jobs
          end
          remove_current_thread
        end
        @workers << next_thread
      end
    end

    protected

    # 이름 충돌 막기 위해 _를 붙인다
    def _loop_execute_jobs
      loop do
        begin
          blk, args = @jobs.pop
          blk.call(*args)
        rescue StandardError, GRPC::Core::CallError => e
          GRPC.logger.warn("Error in worker thread")
          GRPC.logger.warn(e)
        end
        # there shouldn't be any work given to this thread while its busy
        # fail('received a task while busy') unless worker_queue.empty?
        @stop_mutex.synchronize do
          return if @stopped
          # @ready_workers << worker_queue
        end
      end
    end
  end
end
