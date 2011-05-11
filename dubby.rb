#!/usr/bin/env ruby

require 'memcache'
require 'yaml'
require 'thread'

class Dubby
  include Enumerable

  class DubbyError < StandardError
  end
  class TransactionError < DubbyError
  end

  class Connection
    class NetworkError < StandardError
    end
    def initialize()
      @host = @port = @connection = nil
    end
    def get()
      raise(NotImplementedError.new('must be overrideen'))
    end
    def set()
      raise(NotImplementedError.new('must be overrideen'))
    end
    def active?()
      raise(NotImplementedError.new('must be overrideen'))
    end
    private
    def connection()
      @connection
    end
  end
  class DummyConnection < Connection
    def get(*_)
    end
    def set(*_)
    end
    def active?()
      true
    end
  end
  class MemcacheConnection < Connection
    CACHE_EXPIRE_TIME = 60
    def initialize(host, port)
      @host = host
      @port = port
      @connection_lock = Mutex.new
    end
    if MemCache::VERSION.to_f < 1.7
      def get(key)
        begin
          connection.get(key)
        rescue MemCache::MemCacheError => error
          raise(NetworkError.new(error.message))
        end
      end
      def set(key, val, exptime=CACHE_EXPIRE_TIME)
        begin
          connection.set(key, val, exptime)
        rescue MemCache::MemCacheError => error
          raise(NetworkError.new(error.message))
        end
      end
    else
      def get(key)
        begin
          connection.get(key, raw=true)
        rescue MemCache::MemCacheError => error
          raise(NetworkError.new(error.message))
        end
      end
      def set(key, val, exptime=CACHE_EXPIRE_TIME)
        begin
          connection.set(key, val, exptime, raw=true)
        rescue MemCache::MemCacheError => error
          raise(NetworkError.new(error.message))
        end
      end
    end
    def active?()
      connection.active?
    end
    private
    def connection()
      @connection_lock.synchronize {
        if !@connection or !@connection.active?
          begin
            @connection = MemCache.new("#{@host}:#{@port}")
          rescue MemCache::MemCacheError => error
            raise(NetworkError.new(error.message))
          end
        end
      }
      @connection
    end
  end

  class Serializer
  end
  class MarshalSerializer < Serializer
    def serialize(obj)
      Marshal.dump(obj)
    end
    def deserialize(str)
      Marshal.load(str)
    end
  end
  class YAMLSerializer < Serializer
    def serialize(obj)
      begin
        obj.to_yaml
      rescue YAML::Error => error
        raise(DataError.new("serialization failed for #{key} (#{error.message})"))
      end
    end
    def deserialize(str)
      begin
        if str
          YAML.load(str)
        else
          nil
        end
      rescue YAML::Error => error
        raise(DataError.new("deserialization failed for #{key} (#{error.message})"))
      end
    end
  end

  DEFAULT_OPTIONS = {
    :host => 'localhost',
    :port => 11211,
  }.freeze
  GET_RETRIES = 2
  GET_RETRY_WAIT = 0.3
  SET_RETRIES = 4
  SET_RETRY_WAIT = 0.3
  COMMIT_RETRIES = 1
  COMMIT_RETRY_WAIT = 0.3
  ROLLBACK_RETRIES = 16
  ROLLBACK_RETRY_WAIT = 0.3

  def initialize(options={})
    @options = DEFAULT_OPTIONS.merge(options).freeze
    @known_record = {}
    @known_record_lock = Mutex.new
    @uncommitted_record = {}
    @uncommitted_record_lock = Mutex.new
    @transaction_lock = Mutex.new

    if @options.has_key?(:readable_host) and @options.has_key?(:writable_host)
      readable_host = @options[:readable_host]
      readable_port = ( @options[:readable_port] or @options[:port] )
      @readable_connection = MemcacheConnection.new(readable_host, readable_port)
      writable_host = @options[:writable_host]
      writable_port = ( @options[:writable_port] or @options[:port] )
      @writable_connection = MemcacheConnection.new(writable_host, writable_port)
    else
      host = @options[:host]
      port = @options[:port]
      @readable_connection = @writable_connection = MemcacheConnection.new(host, port)
    end

## set up cache_connection only if host and port are specified
    if @options.has_key?(:cache_host) and @options.has_key?(:cache_port)
      cache_host = @options[:cache_host]
      cache_port = @options[:cache_port]
      @cache_connection = MemcacheConnection.new(cache_host, cache_port)
    else
      @cache_connection = DummyConnection.new
    end

    @serializer = YAMLSerializer.new
  end

  def save!()
## try doing things like transaction for given keys
    @transaction_lock.synchronize {
      hash = @uncommitted_record.dup
      source_hash = nil
      begin
        source_hash = begin_transaction(hash.keys)
      rescue => error
        raise(TransactionError.new("failed to start transaction (#{error.message})"))
      end
      begin
        commit_transaction(source_hash, hash)
        hash.each_key { |key| @uncommitted_record_lock.synchronize { @uncommitted_record.delete(key) } }
      rescue => error
        rollback_transaction(source_hash)
        raise(TransactionError.new("failed to commit transaction (#{error.message})"))
      end
    }
  end

  def get(key, use_cache=true)
    @uncommitted_record[key] or get_committed(key, use_cache)
  end
  alias [] get

  def get_committed(key, use_cache=true)
    str = nil
    begin
      if use_cache
        begin
          str = cache_connection.get(key)
        rescue Connection::NetworkError => error
          # just ignore. this is not a problem if we can read it from primary store.
        end
      end
      if str
        if $DEBUG
          STDERR.puts("#{self.class}: reading #{str ? str.size : 0} byte(s) from #{key.dump} (cache)")
        end
      else
        try_times(GET_RETRIES, GET_RETRY_WAIT) {
          str = readable_connection.get(key)
        }
        try_times(SET_RETRIES, SET_RETRY_WAIT) {
          cache_connection.set(key, str)
        }
        if $DEBUG
          STDERR.puts("#{self.class}: reading #{str ? str.size : 0} byte(s) from #{key.dump}")
        end
      end
    rescue Connection::NetworkError => error
      raise(TransactionError.new("failed to read value from #{key} (#{error.message})"))
    end
    val = nil
    begin
      val = deserialize(str)
      append_key(key)
    rescue Serializer::DataError => error
      raise(error)
    end
    val
  end

  def set!(key, val)
    str = nil
    begin
      str = serialize(val)
    rescue Serializer::DataError => error
      raise(error)
    end
    begin
      try_times(SET_RETRIES, SET_RETRY_WAIT) {
        writable_connection.set(key, str)
      }
      try_times(SET_RETRIES, SET_RETRY_WAIT) {
        cache_connection.set(key, str)
      }
      if $DEBUG
        STDERR.puts("#{self.class}: writing #{val ? val.size : 0} byte(s) to #{key.dump}")
      end
      append_key(key)
      val
    rescue Connection::NetworkError => error
      raise(TransactionError.new("failed to write value to #{key} (#{error.message})"))
    end
  end

  def set(key, val)
    append_key(key)
    @uncommitted_record_lock.synchronize {
      @uncommitted_record[key] = val
    }
  end
  alias []= set

  def to_hash()
    @known_record.reject { |_, active| !active }.reduce({}) { |hash, (key, _)| hash.tap { |hash| hash[key] = get(key) } }
  end

  def to_s()
    to_hash.to_s
  end

  def inspect()
    to_hash.inspect
  end

  def to_yaml()
    to_hash.to_yaml
  end

  def append_key(key)
    @known_record_lock.synchronize {
      @known_record[key] = true
    }
  end

  def delete_key(key)
    @known_record_lock.synchronize {
      @known_record[key] = false
    }
  end

  def each(&block)
    to_hash.each(&block)
  end

  def size()
    @known_record.size
  end
  alias length size

  def nitems()
    find_all { |_, val| !val.nil? }.size
  end

  private
  def begin_transaction(keys)
    if $DEBUG
      STDERR.puts("#{self.class}: begin transaction for keys (#{keys.join(', ')})")
    end
    pairs = keys.zip(keys.map { |key| get_committed(key, false) })
    pairs.reduce({}) { |hash, (key, val)| hash.tap { |hash| hash[key] = val } }
  end

  def commit_transaction(source_hash, hash)
    if $DEBUG
      STDERR.puts("#{self.class}: commit transaction for keys (#{hash.keys.join(', ')})")
    end
    hash.each { |key, val|
      source_val = source_hash[key]
      current_val = get_committed(key, false)
      unless source_val == current_val
        raise(TransactionError.new("commit failed for key #{key} (#{source_val.inspect} != #{current_val.inspect})"))
      end
      begin
        if val != current_val
          try_times(COMMIT_RETRIES, COMMIT_RETRY_WAIT) {
            set!(key, val)
          }
        end
      rescue => error
        raise(TransactionError.new("commit failed for key #{key} (#{error.message})"))
      end
    }
    hash
  end

  def rollback_transaction(hash)
    if $DEBUG
      STDERR.puts("#{self.class}: rollback transaction for keys (#{hash.keys.join(', ')})")
    end
    hash.each { |key, val|
      begin
        try_times(ROLLBACK_RETRIES, ROLLBACK_RETRY_WAIT) {
          set!(key, val)
        }
      rescue => error
        raise(TransactionError.new("rollback failed for key #{key} (#{error.message})"))
      end
    }
    hash
  end

  def try_times(max_retries=1, retry_wait=0, &block)
    trial = 0
    while trial < max_retries
      begin
        return block.call(trial)
      rescue => error
        trial += 1
        sleep(rand(retry_wait))
      end
    end
    raise(RuntimeError.new("retry count exceeded (max_retries = #{max_retries})"))
  end

  def readable_connection()
    @readable_connection
  end

  def writable_connection()
    @writable_connection
  end

  def cache_connection()
    @cache_connection
  end

  def serialize(obj)
    return @serializer.serialize(obj)
  end

  def deserialize(str)
    return @serializer.deserialize(str)
  end

end

if $0 == __FILE__
  $DEBUG = true
## use memcached as primary store
  dubby = Dubby.new({:port => 11211})
## use tokyotyrant as primary store and memcached as temporary cache
# dubby = Dubby.new({:port => 1978, :cache_host => 'localhost', :cache_port => 11211})
  p(dubby)
  dubby.set("Test-#{$0}!!foo", "fOo valuE at #{Time.now}")
  dubby.set("tEst-#{$0}!!bar", "BaR valuE at #{Time.now}")
  dubby.set("teSt-#{$0}!!baz", "BAZ VALUE at #{Time.now}")
  p(dubby)
  dubby.save!
  p(dubby)
end
