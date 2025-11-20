module Fx
  # @api private
  module SchemaDumper
    def tables(stream)
      # don't apply Fx schema dumping to non-postgres sql databases
      db_connection_adapter = Fx.configuration.database.instance_variable_get(:@connectable)&.connection
      return super if !db_connection_adapter.is_a?(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter)

      if Fx.configuration.dump_functions_at_beginning_of_schema
        functions(stream)
        casts(stream)
        super
      else
        super
        functions(stream)
        casts(stream)
      end

      triggers(stream)
    end

    private

    def casts(stream)
      dumpable_casts_in_database = Fx.database.casts

      dumpable_casts_in_database.each do |cast|
        stream.puts(cast.to_schema)
      end

      stream.puts if dumpable_casts_in_database.any?
    end

    def functions(stream)
      dumpable_functions_in_database = Fx.database.functions

      dumpable_functions_in_database.each do |function|
        stream.puts(function.to_schema)
      end

      stream.puts if dumpable_functions_in_database.any?
    end

    def triggers(stream)
      dumpable_triggers_in_database = Fx.database.triggers

      if dumpable_triggers_in_database.any?
        stream.puts
      end

      dumpable_triggers_in_database.each do |trigger|
        stream.puts(trigger.to_schema)
      end
    end
  end
end
