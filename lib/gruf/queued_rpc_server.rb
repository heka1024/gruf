# frozen_string_literal: true

require "gruf/pool"

module Gruf
  class QueuedRpcServer < ::GRPC::RpcServer
    def initialize(
      pool_size: DEFAULT_POOL_SIZE,
      max_waiting_requests: DEFAULT_MAX_WAITING_REQUESTS,
      poll_period: DEFAULT_POLL_PERIOD,
      pool_keep_alive: Pool::DEFAULT_KEEP_ALIVE,
      connect_md_proc: nil,
      server_args: {},
      interceptors: []
    )
      super(
        pool_size: pool_size,
        max_waiting_requests: max_waiting_requests || DEFAULT_MAX_WAITING_REQUESTS,
        poll_period: poll_period,
        pool_keep_alive: pool_keep_alive,
        connect_md_proc: connect_md_proc,
        server_args: server_args,
        interceptors: interceptors
      )
      @pool = Gruf::Pool.new(pool_size, keep_alive: pool_keep_alive)
    end

    # Sends RESOURCE_EXHAUSTED if there are too many unprocessed jobs
    def available?(an_rpc)
      job_count = @pool.jobs_waiting

      return an_rpc if job_count < @max_waiting_requests

      ::GRPC.logger.warn("no free worker threads currently")
      noop = proc { |x| x }

      # Create a new active call that knows that metadata hasn't been
      # sent yet
      c = ::GRPC::ActiveCall.new(
        an_rpc.call,
        noop,
        noop,
        an_rpc.deadline,
        metadata_received: true,
        started: false
      )
      c.send_status(
        ::GRPC::Core::StatusCodes::RESOURCE_EXHAUSTED,
        "No free threads in thread pool"
      )
      nil
    end
  end
end
