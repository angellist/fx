module Fx
  # @api private
  class Cast
    include Comparable

    def self.cast_db_name(source_type, target_type)
      "#{source_type} AS #{target_type}"
    end

    def self.cast_ruby_name(source_type, target_type)
      cast_db_name(source_type, target_type).downcase.gsub(' ', '_')
    end

    attr_reader :name
    delegate :<=>, to: :name

    def initialize(row)
      @source_type = row.fetch("source_type")
      @target_type = row.fetch("target_type")
      @function_name = row.fetch("function_name")
      @castcontext = row.fetch("castcontext")
      @castmethod = row.fetch("castmethod")
      @name = self.class.cast_ruby_name(@source_type, @target_type)
    end

    # Indicates how the cast is performed.
    # f means that the function specified in the castfunc field is used.
    # i means that the input/output functions are used.
    # b means that the types are binary-coercible, thus no conversion is required.
    def cast_method
      return '' if @castmethod.nil? || @castmethod.empty?

      case @castmethod
      when 'f'
        "WITH FUNCTION #{@function_name}(#{@source_type})"
      when 'i'
        'WITH INOUT'
      when 'b'
        'WITHOUT FUNCTION'
      else
        ''
      end
    end

    # Indicates what contexts the cast can be invoked in.
    # e means only as an explicit cast (using CAST or :: syntax).
    # a means implicitly in assignment to a target column, as well as explicitly.
    # i means implicitly in expressions, as well as the other cases.
    def cast_context
      return '' if @castcontext.nil? || @castcontext.empty?

      case @castcontext
      when 'e'
        ''
      when 'a'
        'AS ASSIGNMENT'
      when 'i'
        'AS IMPLICIT'
      else
        ''
      end
    end

    def definition
      @definition ||= "CREATE CAST (#{self.class.cast_db_name(@source_type, @target_type)}) #{cast_method} #{cast_context};"
    end

    def ==(other)
      name == other.name && definition == other.definition
    end

    def to_schema
      <<~SCHEMA.indent(2)
        create_cast :#{@source_type}, :#{@target_type}, sql_definition: <<-'SQL'
        #{definition.indent(4).rstrip}
        SQL
      SCHEMA
    end
  end
end
