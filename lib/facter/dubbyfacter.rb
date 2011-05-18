#!/usr/bin/env ruby

require 'dubby'

def load_dubby()
  dubby_conf = File.join('/etc', "dubby.conf")
  loaded_config = YAML.load(File.read(dubby_conf))
  config = loaded_config.keys.reduce({}) { |hash, key| hash.tap { |hash| hash[key.to_sym] = loaded_config[key] } }

  dubby = Dubby.new(config)

  dubby.keys.each { |key|
    begin
      if value = dubby.get(key)
        symbol = "dubby_#{key.gsub(/\-|\/|:/, '_')}".to_sym
        Facter.add(symbol) { setcode { value.to_a.join(',') } }
      end
    rescue => error
      STDERR.puts("failed to load key from dubby: #{key} (#{error.message})")
      next
    end
  }
end

begin
  load_dubby
rescue => error
  puts "#{File.basename(__FILE__)} not loaded (#{error.message})"
end
