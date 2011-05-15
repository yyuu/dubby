#!/usr/bin/env ruby

require 'thread'
require 'forwardable'

class DubbyStore
  include Enumerable

  class Error < StandardError
  end
  class TransactionError < Error
  end

  class Connection
    class NetworkError < StandardError
    end
    def initialize()
      @connection = nil
    end
    def delete()
      raise(NotImplementedError.new('should be overridden'))
    end
    def get()
      raise(NotImplementedError.new('should be overridden'))
    end
    def set()
      raise(NotImplementedError.new('should be overridden'))
    end
    def active?()
      raise(NotImplementedError.new('should be overridden'))
    end
    private
    def connection()
      @connection
    end
  end

  class DummyConnection < Connection
    def delete(key)
      nil
    end
    def get(key)
      nil
    end
    def set(key, val)
      val
    end
    def active?()
      true
    end
  end

  class MemcacheConnection < Connection
    require 'memcache'

    CACHE_EXPIRY = 86400
    def initialize(host, port, cache_expiry=nil)
      @host = host
      @port = port
      @cache_expiry = ( cache_expiry or CACHE_EXPIRY )

      begin
        @connection = MemCache.new("#{@host}:#{@port}")
      rescue MemCache::MemCacheError => error
        raise(NetworkError.new(error.message))
      end
    end
    def delete(key)
      begin
        connection.delete(key)
      rescue MemCache::MemCacheError => error
        raise(NetworkError.new(error.message))
      end
    end
    if MemCache::VERSION.to_f < 1.7
      def get(key)
        begin
          connection.get(key)
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
    end
    if MemCache::VERSION.to_f < 1.7
      def set(key, val)
        begin
          connection.set(key, val, @cache_expiry)
          val
        rescue MemCache::MemCacheError => error
          raise(NetworkError.new(error.message))
        end
      end
    else
      def set(key, val)
        begin
          connection.set(key, val, @cache_expiry, raw=true)
          val
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
      @connection
    end
  end

  class HashConnection
    def initialize()
      @hash = {}
    end
    def delete(key)
      @hash.delete(key)
    end
    def get(key)
      @hash[key]
    end
    def set(key, val)
      @hash[key] = val
    end
    def active?()
      true
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
    require 'yaml'
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

    if @options.has_key?(:readonly_host)
      readonly_host = @options[:readonly_host]
      readonly_port = ( @options[:readonly_port] or @options[:port] )
      writable_host = @options[:host]
      writable_port = @options[:port]
      case @options[:protocol]
      when /dummy/i
        @readonly_connection = DummyConnection.new
        @writable_connection = DummyConnection.new
      when /hash/i
        @readonly_connection = HashConnection.new
        @writable_connection = HashConnection.new
      else
        @readonly_connection = MemcacheConnection.new(readonly_host, readonly_port)
        @writable_connection = MemcacheConnection.new(writable_host, writable_port)
      end
    else
      host = @options[:host]
      port = @options[:port]
      case @options[:protocol]
      when /dummy/i
        @readonly_connection = @writable_connection = DummyConnection.new
      when /hash/i
        @readonly_connection = @writable_connection = HashConnection.new
      else
        @readonly_connection = @writable_connection = MemcacheConnection.new(host, port)
      end
    end

## set up cache_connection only if host and port are specified
    if @options.has_key?(:cache_host) and @options.has_key?(:cache_port)
      cache_host = @options[:cache_host]
      cache_port = @options[:cache_port]
      case @options[:cache_protocol]
      when /dummy/i
        @cache_connection = DummyConnection.new
      when /hash/i
        @cache_connection = HashConnection.new
      else
        @cache_connection = MemcacheConnection.new(cache_host, cache_port, @options[:cache_expiry])
      end
    else
      @cache_connection = DummyConnection.new
    end

    case @options[:serializer]
    when /marshal/i
      @serializer = MarshalSerializer.new
    else
      @serializer = YAMLSerializer.new
    end
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

  def get(key)
    @uncommitted_record[key] or get_committed(key)
  end
  alias [] get

  def get_committed(key)
    str = nil
    begin
      str = _get_record(key)
    rescue Connection::NetworkError => error
      raise(TransactionError.new("failed to read value from #{key} (#{error.message})"))
    end
    val = nil
    begin
      val = deserialize(str)
      register(key)
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
      _set_record!(key, str)
      register(key)
    rescue Connection::NetworkError => error
      raise(TransactionError.new("failed to write value to #{key} (#{error.message})"))
    end
    val
  end

  def set(key, val)
    register(key)
    @uncommitted_record_lock.synchronize {
      @uncommitted_record[key] = val
    }
  end
  alias []= set

  def delete!(key)
    begin
      delete(key)
      _delete_record!(key)
    rescue => error
      raise(TransactionError.new("failed to delete key #{key} (#{error.message})"))
    end
  end

  def delete(key)
    @uncommitted_record_lock.synchronize {
      @uncommitted_record[key] = nil # tombstoned
    }
    deregister(key)
  end

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

  def register(*keys)
    @known_record_lock.synchronize {
      keys.each { |key|
        @known_record[key] = true
      }
    }
  end

  def deregister(*keys)
    @known_record_lock.synchronize {
      keys.each { |key|
        @known_record[key] = false
      }
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

  def keys()
    @known_record.keys
  end

  private
  def begin_transaction(keys)
    if $DEBUG
      STDERR.puts("#{self.class}: begin transaction for keys (#{keys.join(', ')})")
    end
    pairs = keys.zip(keys.map { |key| _get_primary_record(key) })
    pairs.reduce({}) { |hash, (key, val)| hash.tap { |hash| hash[key] = val } }
  end

  def commit_transaction(source_hash, hash)
    if $DEBUG
      STDERR.puts("#{self.class}: commit transaction for keys (#{hash.keys.join(', ')})")
    end
    hash.each { |key, val|
      source_val = source_hash[key]
      current_val = _get_primary_record(key)
      unless source_val == current_val
        raise(TransactionError.new("commit failed for key #{key} (#{source_val.inspect} != #{current_val.inspect})"))
      end
      begin
        if val != current_val
          if val.nil?
            delete!(key) # delete tombstoned keys
          else
            try_times(COMMIT_RETRIES, COMMIT_RETRY_WAIT) {
              set!(key, val)
            }
          end
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

  def readonly_connection()
    @readonly_connection
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

  def _get_record(key)
## use cache as first resolver, send request to primary if key is not cached.
#   begin
#     _get_cached_record(key)
#   rescue => error
#     _get_primary_record(key).tap { |val|
#       begin
#         _set_cached_record(key, val) if val
#       rescue => error
#         # nop
#       end
#     }
#   end

## use primary as first resolver, send request to secondary only if primary is down.
    begin
      _get_primary_record(key).tap { |val|
        begin
          _set_cached_record!(key, val) if val
        rescue => error
          # nop
        end
      }
    rescue => error
      _get_cached_record(key)
    end
  end

  def _get_primary_record(key)
    readonly_connection.get(key).tap { |val|
      if $DEBUG
        STDERR.puts("#{self.class}: read #{val ? val.size : 0} byte(s) from #{key.dump}")
      end
    }
  end

  def _get_cached_record(key)
    cache_connection.get(key).tap { |val|
      if $DEBUG
        STDERR.puts("#{self.class}: read #{val ? val.size : 0} byte(s) from #{key.dump} on cache")
      end
    }
  end

  def _set_record!(key, val)
## write through
    _set_primary_record!(key, val)
    _set_cached_record!(key, val)
  end

  def _set_primary_record!(key, val)
    writable_connection.set(key, val).tap { |val|
      if $DEBUG
        STDERR.puts("#{self.class}: wrote #{val ? val.size : 0} byte(s) to #{key.dump}")
      end
    }
  end

  def _set_cached_record!(key, val)
    cache_connection.set(key, val).tap { |val|
      if $DEBUG
        STDERR.puts("#{self.class}: wrote #{val ? val.size : 0} byte(s) to #{key.dump} on cache")
      end
    }
  end

  def _delete_record!(key)
    _delete_primary_record!(key).tap {
      begin
        _delete_cached_record!(key)
      rescue => error
        # nop
      end
    }
  end

  def _delete_primary_record!(key)
    writable_connection.delete(key).tap {
      if $DEBUG
        STDERR.puts("#{self.class}: delete a key of #{key.dump}")
      end
    }
  end

  def _delete_cached_record!(key)
    cache_connection.delete(key).tap {
      if $DEBUG
        STDERR.puts("#{self.class}: delete a key of #{key.dump} on cache")
      end
    }
  end
end

class DubbyManager
  extend Forwardable
  def initialize(store, options={})
    @store = store
    @options = options
    bootstrap(@options)
  end
  def get(key)
    raise(NotImplementedError.new('should be overridden'))
  end
  def pop(key, val)
    raise(NotImplementedError.new('should be overridden'))
  end
  def push(key, val)
    raise(NotImplementedError.new('should be overridden'))
  end
  def set(key, val)
    raise(NotImplementedError.new('should be overridden'))
  end
  def set!(key, val)
    raise(NotImplementedError.new('should be overridden'))
  end
  def delete(key)
    raise(NotImplementedError.new('should be overridden'))
  end
  def delete!(key)
    raise(NotImplementedError.new('should be overridden'))
  end
  def_delegator :@store, :keys
  def_delegator :@store, :save!
  def_delegator :@store, :to_hash
  private
  def bootstrap(options={})
  end
end

class DubbySimpleManager < DubbyManager
  extend Forwardable
  def_delegator :@store, :delete
  def_delegator :@store, :delete!
  def_delegator :@store, :get
  def_delegator :@store, :pop
  def_delegator :@store, :push
  def_delegator :@store, :set
  def_delegator :@store, :set!
end

class DubbyTier1Manager < DubbyManager
  INITIAL_KEY = "dubby_initial_key"
  def delete(key)
    if @known_keys.include?(key) or key == @initial_key
      @known_keys.delete(key)
      @store.set(@initial_key, @known_keys)
      @store.delete(key)
    else
      raise("unknown key: #{key}")
    end
  end
  def delete!(key)
    if @known_keys.include?(key) or key == @initial_key
      @known_keys.delete(key)
      @store.set!(@initial_key, @known_keys)
      @store.delete!(key)
    else
      raise("unknown key: #{key}")
    end
  end
  def get(key)
    if @known_keys.include?(key) or key == @initial_key
      @store.get(key)
    else
      raise("unknown key: #{key}")
    end
  end
  def pop(key, val)
    if @known_keys.include?(key) or key == @initial_key
      list = ( get(key) or [] )
    else
      list = []
    end
    if list.is_a?(Array)
      if list.include?(val)
        list.delete(val)
        set(key, list)
      end
    else
      raise(TypeError.new("not an array: (#{list.inspect})"))
    end
  end
  def push(key, val)
    if @known_keys.include?(key) or key == @initial_key
      list = ( get(key) or [] )
    else
      list = []
    end
    if list.is_a?(Array)
      unless list.include?(val)
        list.push(val)
        set(key, list)
      end
    else
      raise(TypeError.new("not an array: (#{list.inspect})"))
    end
  end
  def set(key, val)
    if @known_keys.include?(key)
      @store.set(key, val)
    else
      @known_keys.push(key)
      @store.set(@initial_key, @known_keys)
      @store.set(key, val)
    end
  end
  def set!(key, val)
    if @known_keys.include?(key)
      @store.set!(key, val)
    else
      @known_keys.push(key)
      @store.set!(@initial_key, @known_keys)
      @store.set!(key, val)
    end
  end
  private
  def bootstrap(options={})
    @initial_key = ( options[:initial_key] or INITIAL_KEY )
    @known_keys = @store.get(@initial_key)
    if !@known_keys or !@known_keys.is_a?(Array)
      @known_keys = []
    end
    if @known_keys.include?(@initial_key)
      raise("loop detected in #{@initial_key}")
    end
    @store.register(*@known_keys)
  end
end

class DubbyListManager < DubbyManager
  INITIAL_KEY = 'dubby_initial_key'
end

class DubbyTreeManager < DubbyManager
  INITIAL_KEY = 'dubby_initial_key'
end

class Dubby
  extend Forwardable
  def initialize(options={})
    @store = DubbyStore.new(options)
    case options[:manager]
    when /simple/i
      @manager = DubbySimpleManager.new(@store, options)
    else
      @manager = DubbyTier1Manager.new(@store, options)
    end
  end
  def_delegator :@manager, :delete
  def_delegator :@manager, :delete!
  def_delegator :@manager, :get
  def_delegator :@manager, :keys
  def_delegator :@manager, :pop
  def_delegator :@manager, :push
  def_delegator :@manager, :save!
  def_delegator :@manager, :set
  def_delegator :@manager, :set!

  def_delegator :@store, :inspect
  def_delegator :@store, :to_s
  def_delegator :@store, :to_hash
end

if $0 == __FILE__
  $DEBUG = true
## use memcached as primary store
## use tokyotyrant as primary store and memcached as temporary cache
  options = {
    :port => 1978,
    :cache_host => 'localhost',
    :cache_port => 11211,
    :initial_key => 'dubby_initial_key',
#   :protocol => 'memcache',
#   :cache_protocol => 'memcache',
#   :manager => 'tier1',
#   :serializer => 'yaml',
  }
  dubby = Dubby.new(options)
  p(dubby)
  dubby.set("dubby_foo", "foo on #{Time.now}")
  dubby.set("dubby_bar", "bar on #{Time.now}")
  dubby.set("dubby_baz", "baz on #{Time.now}")
  p(dubby)
  dubby.save!
  p(dubby)
end

# vim:set ft=ruby :
