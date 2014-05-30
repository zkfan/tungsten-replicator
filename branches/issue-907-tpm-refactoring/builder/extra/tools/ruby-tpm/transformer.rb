#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

# Simple utility class to transform files using regular expressions
require 'system_require'
system_require 'date'

class Transformer
  DEFAULT_VALUE = "{default}"
  
  def add_fixed_replacement(key, value)
    if value == nil
      raise("Unable to add a fixed replacement using a nil value")
    end
    
    if value == DEFAULT_VALUE
      @fixed_replacements.delete(key)
    else
      @fixed_replacements[key] = value
    end
  end
  
  def add_fixed_addition(key, value)
    if value == nil
      raise("Unable to add a fixed addition using a nil value")
    end
    
    if value == DEFAULT_VALUE
      @fixed_additions.delete(key)
    else
      @fixed_additions[key] = value
    end
  end
  
  def add_fixed_match(key, value)
    if value == nil
      raise("Unable to add a fixed match using a nil value")
    end
    
    if value == DEFAULT_VALUE
      @fixed_matches.delete(key)
      return
    end
      
    match = value.split("/")
    match.shift
    
    if match.size != 2
      raise("Unable to add a fixed match using '#{value}'.  Matches must be in the form of /search/replacement/.")
    end
    
    @fixed_matches[key] = match
  end
  
  def set_fixed_properties(fixed_properties = [])
    @fixed_replacements = {}
    @fixed_additions = {}
    @fixed_matches = {}
    
    if fixed_properties == nil
      fixed_properties = []
    end
    
    fixed_properties.each{
      |val|
      
      unless val =~ /=/
        raise "Invalid value #{val} given for '--property'.  There should be a key/value pair joined by a single =."
      end
      
      val_parts = val.split("=")
      last_char=val_parts[0][-1,1]
      key=val_parts.shift()
      
      key_parts = key.split(":")
      if key_parts.size() == 2
        unless @outfile && @outfile.include?(key_parts[0])
          next
        end
        
        key = key_parts[1]
      elsif key_parts.size() == 1
        # Do nothing
      else
        raise "Invalid search key #{key} given for '--property'. There may only be a single ':' in the search key."
      end
        
      
      if last_char == "+"
        add_fixed_addition(key[0..-2], val_parts.join("="))
      elsif last_char == "~"
        add_fixed_match(key[0..-2], val_parts.join("="))
      else
        add_fixed_replacement(key, val_parts.join("="))
      end
    }
  end
  
  def initialize(config, outfile = nil)
    @config = config
    @outfile = outfile
    @transform_values_method = nil

    @mode = nil
    @timestamp = true
    @watch_file = true
    
    @fixed_replacements = {}
    @fixed_additions = {}
    @fixed_matches = {}
    
    @output = []
  end
  
  # Enable/Disable the header timestamp of when the file was created
  def timestamp?(v = nil)
    if v != nil
      @timestamp = v
    end
    
    @timestamp
  end
  
  # Enable/Disable support for identifying user modifications to this file
  def watch_file?(v = nil)
    if v != nil
      @watch_file = v
    end
    
    @watch_file
  end
  
  # Force the file to have a specific mode
  def mode(v = nil)
    if v != nil
      @mode = v
    end
    
    @mode
  end
  
  def set_transform_values_method(func)
    @transform_values_method = func
  end

  # Evaluate each line in the template by passing it to a code block 
  # and storing the result
  def transform_lines(&block)
    @output.map!{
      |line|
      block.call(line)
    }
  end
    
  # Evaluate a single template line and return the result
  def transform_line(line)
    # Look for --property=key=value options that will apply to this line
    line_keys = line.scan(/^[#]?([a-zA-Z0-9\._-]+)[ ]*=[ ]*.*/)
    if line_keys.length() > 0 && @fixed_replacements.has_key?(line_keys[0][0])
      "#{line_keys[0][0]}=#{@fixed_replacements[line_keys[0][0]]}"
    else
      # Look for template placeholders
      # ([\#|include|includeAll]*\()? -> A function reference to do something besides a simple substitution
      # ([A-Za-z\._]+) -> The actual template variable name
      # (\|)? -> A separator that identifies a default value
      # ([A-Za-z0-9\._\-\=\?\&]*)? -> The default value if the template variable does not have a value
      line.gsub!(/@\{([\#|include|includeAll]*\()?([A-Za-z\._]+)(\|)?([A-Za-z0-9\._\-\=\?\&]*)?[\)]?\}/){
        |match|
        functionMarker = $1
        defaultValueMarker = $3
        defaultValue = $4
        # This must be after the other lines so it doesn't overwrite the special variables
        r = @transform_values_method.call($2.split("."))
        
        # Replace this line with the content of a template 
        # returned by the template variable
        if functionMarker == "include("
          r = transform_file(find_template(r))
        # Replace this line with the content of all templates found based
        # on a list of search patterns returned by the template variable
        elsif functionMarker == "includeAll("
          patterns = r
          r = []
          find_templates(patterns).each {
            |template|
            r << transform_file(template)
            r << ""
          }
          
          r = r.join("\n")
        else
          if r.is_a?(Array)
            r = r.join(',')
          end
        
          # There is no returned value and a default is available
          if r.to_s() == "" && defaultValueMarker == "|"
            r = defaultValue
          end
        
          # This function will create a comment if the template variable is empty
          if functionMarker == "#("
            if r.to_s() == ""
              r = "#"
            else
              r = ""
            end
          end
        end
        
        r
      }
      
      line_keys = line.scan(/^[#]?([a-zA-Z0-9\._-]+)[ ]*=(.*)/)
      if line_keys.size > 0
        # Append content to this line based on the --property=key+=value
        if @fixed_additions.has_key?(line_keys[0][0])
          line_keys[0][1] += @fixed_additions[line_keys[0][0]]
          line = line_keys[0][0] + "=" + line_keys[0][1]
        end
      
        # Modify content on this line based on the --property=key~=value
        if @fixed_matches.has_key?(line_keys[0][0])
          line_keys[0][1].sub!(Regexp.new(@fixed_matches[line_keys[0][0]][0]), @fixed_matches[line_keys[0][0]][1])
          line = line_keys[0][0] + "=" + line_keys[0][1]
        end
      end
      
      line
    end
  end
  
  # Read the contents of a file and transform each line using the current Transformer
  def transform_file(path)
    out = []
    File.open(path) do |file|
      while line = file.gets
        out << transform_line(line.chomp())
      end
    end
    
    return out.join("\n")
  end
  
  # Evaluate the template and push the contents to the outfile
  # If no file was given then the contents are returned as a string
  def output
    @output.map!{
      |line|
      transform_line(line)
    }
    
    if @outfile
      Configurator.instance.info("Writing " + @outfile)
      File.open(@outfile, "w") {
        |f|
        if mode() != nil
          f.chmod(mode())
        end
        
        if timestamp?()
          f.puts "# AUTO-GENERATED: #{DateTime.now}"
        end

        @output.each{
          |line| 
          f.puts(line) 
        }
      }
      
      if watch_file?()
        Configurator.instance.watch_file(@outfile, @config)
      end
    else
      return self.to_s
    end
  end
  
  # Find a template matching the given pattern and store the contents
  # to be evaluated later
  def set_template(pattern)
    path = find_template(pattern)
    File.open(path) do |file|
      @output = []
      while line = file.gets
        @output << line.chomp()
      end
    end
  end
  
  # Find a physical file that matches the given pattern in the available
  # template search directories
  def find_template(pattern)
    template_path = nil
    get_search_directories().each {
      |dir|
      path = File.expand_path(pattern, dir)
      if File.exists?(path)
        template_path = path
        break
      end
    }
    
    if template_path == nil
      raise MessageError.new("Unable to find a template file for '#{pattern}'")
    end
    template_path
  end
  
  # Find a list of files matching the given search patterns among the available
  # template search directories. Only the first file for each basename
  # will be returned.
  def find_templates(search)
    templates = {}
    
    get_search_directories().each {
      |dir|
      search.each {
        |pattern|
        Dir.glob("#{dir}/#{pattern}") {
          |file|
          base = File.basename(file)
          
          # Do not store the file if it is a duplicate of another template
          unless templates.has_key?(base)
            templates[base] = file
          end
        }
      }
    }
    
    # Sort and return the files as an array
    template_files = []
    templates.keys().sort().each{
      |k|
      template_files << templates[k]
    }
    template_files
  end
  
  # Get an array of directories that may contain template files
  def get_search_directories
    dirs = []
    
    # Add any additional search paths that may be provided
    @config.getProperty(TEMPLATE_SEARCH_PATH).split(",").each {
      |path|
      if File.exists?(path) && File.directory?(path)
        dirs << path
      end
    }
    
    # These are the fallback locations for templates
    dirs << "#{@config.getProperty(HOME_DIRECTORY)}/share/templates"
    dirs << @config.getProperty(PREPARE_DIRECTORY)
    dirs
  end
  
  def get_filename
    @outfile
  end
  
  def to_s
    return to_a.join("\n")
  end
  
  def to_a
    return @output
  end
  
  def <<(line)
    @output << line
  end
end