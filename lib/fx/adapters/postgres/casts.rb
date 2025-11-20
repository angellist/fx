require "fx/cast"
require "fx/adapters/postgres/query_executor"

module Fx
  module Adapters
    class Postgres
      # Fetches defined casts from the postgres connection.
      # @api private
      class Casts
        # The SQL query used by F(x) to retrieve the casts considered dumpable into `db/schema.rb`.
        CASTS_QUERY = <<~SQL
          SELECT
            pt_source.typname as source_type,
            pt_target.typname as target_type,
            pp.proname as function_name,
            castcontext,
            castmethod
          FROM pg_cast
          JOIN pg_type pt_source
            ON pt_source.oid = pg_cast.castsource
          JOIN pg_type pt_target
            ON pt_target.oid = pg_cast.casttarget
          JOIN pg_proc pp
            ON pp.oid = pg_cast.castfunc
          JOIN pg_namespace pn
            ON pn.oid = pp.pronamespace
          LEFT JOIN pg_depend pd
            ON pd.objid = pp.oid AND pd.deptype = 'e'
          LEFT JOIN pg_aggregate pa
            ON pa.aggfnoid = pp.oid
          WHERE pn.nspname = ANY (current_schemas(false))
            AND pd.objid IS NULL
            AND pa.aggfnoid IS NULL
          ORDER BY pg_cast.oid;
        SQL

        # Wraps #all as a static facade.
        #
        # @return [Array<Fx::Cast>]
        def self.all(connection)
          QueryExecutor.call(
            connection: connection,
            query: CASTS_QUERY,
            model_class: Fx::Cast
          )
        end
      end
    end
  end
end
