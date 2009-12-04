module EMogileFS
  
  @@connections = {}
  
  class << self
    def <<(request)
      connection(request.client).new_task request
    end    
    def connection(client)
      @@connections[client] ||= EM.attach client.send(:socket), EMogileFS, client
    end
  end
  
  attr_accessor :client, :current_task
  
  def initialize(client)
    @queue = Queue.new
    @current_task = nil
    self.client = client
    super
  end
  
  def new_task(request)
    @queue << request
    handle_task
  end
  
  def task_completed
    @current_task = nil
  end
  
  def handle_task

    if @queue.size > 0 && @current_task.nil?
      @current_task = @queue.pop
      @current_task.perform
    end
  end
  
  def receive_data(data)
    begin
      raise MogileFS::Backend::ConnectionLost unless data    
      if current_task && current_task.callback
        begin
          parsed = client.send :parse_response, data 
          current_task.callback.call parsed
        rescue MogileFS::Backend::ChannelNotFoundError => e
          puts 'not found'
        end
      end
    ensure
      task_completed
      handle_task
    end
  end
  
  def unbind
    detach
  end
  
end

class EMogileFS::Request
  attr_accessor :request, :client, :socket, :options
  def initialize(client, request)
    self.client, self.request = client, request
    self.socket = client.send :socket
    EMogileFS << self
  end
  
  def callback(options = {}, &block)
    if block
      @options = options
      @callback = block
    else
      @callback
    end
  end
  
  def perform
    begin
      bytes_sent = socket.send request, 0
    rescue SystemCallError
      client.send :shutdown
      raise MogileFS::UnreachableBackendError
    end

    unless bytes_sent == request.length then
      raise MogileFS::RequestTruncatedError,
        "request truncated (sent #{bytes_sent} expected #{request.length})"
    end
    
  end
end

class MogileFS::MogileFS
  def async(method, *args, &block)
    Thread.current[:mogile_async] = true
    req = self.send method, *args
    req.callback(&block)
  end
end
class MogileFS::Backend
  alias do_request_without_async do_request
  
  def do_request(*args)
    if Thread.current[:mogile_async]
      Thread.current[:mogile_async] = false
      do_request_with_async(*args)
    else
      do_request_without_async(*args)
    end
  end
  
  def do_request_with_async(cmd, args)
    request = make_request cmd, args
    EMogileFS::Request.new(self, request)
  end
end