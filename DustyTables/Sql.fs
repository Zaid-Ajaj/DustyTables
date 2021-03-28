namespace DustyTables

open System
open System.Threading.Tasks
open System.Data
open Microsoft.Data.SqlClient
open System.Threading
open FSharp.Control.Tasks

type Sql() =
    static member dbnull = SqlParameter(Value=DBNull.Value)

    static member int(value: int) = SqlParameter(Value = value, DbType = DbType.Int32)

    static member intOrNone(value: int option) =
        match value with
        | Some value -> Sql.int(value)
        | None -> Sql.dbnull

    static member string(value: string) = SqlParameter(Value = value, DbType = DbType.String)

    static member stringOrNone(value: string option) =
        match value with
        | Some value -> Sql.string(value)
        | None -> Sql.dbnull

    static member bool(value: bool) = SqlParameter(Value=value, DbType=DbType.Boolean)

    static member boolOrNone(value: bool option) =
        match value with
        | Some thing -> Sql.bool(thing)
        | None -> Sql.dbnull

    static member double(value: double) = SqlParameter(Value=value, DbType = DbType.Double)

    static member doubleOrNone(value: double option) =
        match value with
        | Some value -> Sql.double(value)
        | None -> SqlParameter(Value=DBNull.Value)

    static member decimal(value: decimal) = SqlParameter(Value=value, DbType = DbType.Decimal)

    static member decimalOrNone(value: decimal option) =
        match value with
        | Some value -> Sql.decimal(value)
        | None -> Sql.dbnull

    static member int16(value: int16) = SqlParameter(Value = value, DbType = DbType.Int16)

    static member int16OrNone(value: int16 option) =
        match value with
        | Some value -> Sql.int16 value
        | None -> Sql.dbnull

    static member int64(value: int64) = SqlParameter(Value = value, DbType = DbType.Int64)

    static member int64OrNone(value: int64 option) =
        match value with
        | Some value -> Sql.int64 value
        | None -> Sql.dbnull

    static member dateTime(value: DateTime) = SqlParameter(Value=value, DbType = DbType.DateTime)

    static member dateTimeOrNone(value: DateTime option) =
        match value with
        | Some value -> Sql.dateTime(value)
        | None -> Sql.dbnull

    static member dateTimeOffset(value: DateTimeOffset) = SqlParameter(Value=value, DbType = DbType.DateTimeOffset)

    static member dateTimeOffsetOrNone(value: DateTimeOffset option) =
        match value with
        | Some value -> Sql.dateTimeOffset(value)
        | None -> Sql.dbnull

    static member uniqueidentifier(value: Guid) = SqlParameter(Value=value, DbType = DbType.Guid)

    static member uniqueidentifierOrNone(value: Guid option) =
        match value with
        | Some value -> Sql.uniqueidentifier value
        | None -> Sql.dbnull

    static member bytes(value: byte[]) = SqlParameter(Value=value)
    static member bytesOrNone(value: byte[] option) =
        match value with
        | Some value -> Sql.bytes value
        | None -> Sql.dbnull

    static member inline table(typeName: string, value: DataTable) =
        SqlParameter(Value = value, TypeName = typeName, SqlDbType = SqlDbType.Structured)

    static member parameter(genericParameter: SqlParameter) = genericParameter

[<RequireQualifiedAccess>]
module Sql =

    type SqlProps = {
        ConnectionString : string
        SqlQuery : string option
        Parameters : (string * SqlParameter) list
        IsFunction : bool
        Timeout: int option
        NeedPrepare : bool
        CancellationToken: CancellationToken
        ExistingConnection : SqlConnection option
    }

    let private defaultProps() = {
        ConnectionString = "";
        SqlQuery = None
        Parameters = [];
        IsFunction = false
        NeedPrepare = false
        Timeout = None
        CancellationToken = CancellationToken.None
        ExistingConnection = None
    }

    let connect constr  = { defaultProps() with ConnectionString = constr }
    let existingConnection (connection: SqlConnection) = { defaultProps() with ExistingConnection = connection |> Option.ofObj }
    let query (sqlQuery: string) props = { props with SqlQuery = Some sqlQuery }
    let queryStatements (sqlQuery: string list) props = { props with SqlQuery = Some (String.concat "\n" sqlQuery) }
    let storedProcedure (sqlQuery: string) props = { props with SqlQuery = Some sqlQuery; IsFunction = true }
    let prepare  props = { props with NeedPrepare = true}
    let parameters ls props = { props with Parameters = ls }
    let timeout n props = { props with Timeout = Some n }

    let populateRow (cmd: SqlCommand) (row: (string * SqlParameter) list) =
        for (parameterName, parameter) in row do
            // prepend param name with @ if it doesn't already
            let normalizedName =
                if parameterName.StartsWith("@")
                then parameterName
                else sprintf "@%s" parameterName

            parameter.ParameterName <- normalizedName
            ignore (cmd.Parameters.Add(parameter))

    let private getConnection (props: SqlProps): SqlConnection =
        match props.ExistingConnection with
        | Some connection -> connection
        | None -> new SqlConnection(props.ConnectionString)

    let private populateCmd (cmd: SqlCommand) (props: SqlProps) =
        if props.IsFunction then cmd.CommandType <- CommandType.StoredProcedure

        match props.Timeout with
        | Some timeout -> cmd.CommandTimeout <- timeout
        | None -> ()

        populateRow cmd props.Parameters

    let executeTransaction queries (props: SqlProps) =
        try
            if List.isEmpty queries
            then Ok [ ]
            else
            let connection = getConnection props
            try
                if not (connection.State.HasFlag ConnectionState.Open)
                then connection.Open()
                use transaction = connection.BeginTransaction()
                let affectedRowsByQuery = ResizeArray<int>()
                for (query, parameterSets) in queries do
                    if List.isEmpty parameterSets
                    then
                       use command = new SqlCommand(query, connection, transaction)
                       let affectedRows = command.ExecuteNonQuery()
                       affectedRowsByQuery.Add affectedRows
                    else
                      for parameterSet in parameterSets do
                        use command = new SqlCommand(query, connection, transaction)
                        populateRow command parameterSet
                        let affectedRows = command.ExecuteNonQuery()
                        affectedRowsByQuery.Add affectedRows

                transaction.Commit()
                Ok (List.ofSeq affectedRowsByQuery)
            finally
                if props.ExistingConnection.IsNone
                then connection.Dispose()

        with
        | error -> Error error

    let executeTransactionAsync queries (props: SqlProps) : Task<int list> =
        task {
            let! token = Async.CancellationToken
            use mergedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token, props.CancellationToken)
            let mergedToken = mergedTokenSource.Token
            if List.isEmpty queries
            then return [ ]
            else
            let connection = getConnection props
            try
                if not (connection.State.HasFlag ConnectionState.Open)
                then do! connection.OpenAsync mergedToken
                use transaction = connection.BeginTransaction ()
                let affectedRowsByQuery = ResizeArray<int>()
                for (query, parameterSets) in queries do
                    if List.isEmpty parameterSets
                    then
                        use command = new SqlCommand(query, connection, transaction)
                        let! affectedRows = command.ExecuteNonQueryAsync mergedToken
                        affectedRowsByQuery.Add affectedRows
                    else
                        for parameterSet in parameterSets do
                            use command = new SqlCommand(query, connection, transaction)
                            populateRow command parameterSet
                            let! affectedRows = command.ExecuteNonQueryAsync mergedToken
                            affectedRowsByQuery.Add affectedRows
                transaction.Commit()
                return List.ofSeq affectedRowsByQuery
            finally
                if props.ExistingConnection.IsNone
                then connection.Dispose()
        }

    let execute (read: RowReader -> 't) (props: SqlProps) : 't list =
        if props.SqlQuery.IsNone then failwith "No query provided to execute. Please use Sql.query"
        let connection = getConnection props
        try
            if not (connection.State.HasFlag ConnectionState.Open)
            then connection.Open()
            use command = new SqlCommand(props.SqlQuery.Value, connection)
            do populateCmd command props
            if props.NeedPrepare then command.Prepare()
            use reader = command.ExecuteReader()
            let rowReader = RowReader(reader)
            let result = ResizeArray<'t>()
            while reader.Read() do result.Add (read rowReader)
            List.ofSeq result
        finally
            if props.ExistingConnection.IsNone
            then connection.Dispose()

    let executeRow (read: RowReader -> 't) (props: SqlProps) : 't =
        if Option.isNone props.SqlQuery then failwith "No query provided to execute. Please use Sql.query"
        let connection = getConnection props
        try
            if not (connection.State.HasFlag ConnectionState.Open)
            then connection.Open()
            use command = new SqlCommand(props.SqlQuery.Value, connection)
            do populateCmd command props
            if props.NeedPrepare then command.Prepare()
            use reader = command.ExecuteReader()
            let rowReader = RowReader(reader)
            if reader.Read()
            then read rowReader
            else failwith "Expected at least one row to be returned from the result set. Instead the result set returned was empty"
        finally
            if props.ExistingConnection.IsNone
            then connection.Dispose()

    let executeRowAsync (read: RowReader -> 't) (props: SqlProps) : Task<'t> =
        task {
            let! token =  Async.CancellationToken
            use mergedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token, props.CancellationToken)
            let mergedToken = mergedTokenSource.Token
            if Option.isNone props.SqlQuery then failwith "No query provided to execute. Please use Sql.query"
            let connection = getConnection props
            try
                if not (connection.State.HasFlag ConnectionState.Open)
                then do! connection.OpenAsync(mergedToken)
                use command = new SqlCommand(props.SqlQuery.Value, connection)
                do populateCmd command props
                if props.NeedPrepare then command.Prepare()
                use! reader = command.ExecuteReaderAsync(mergedToken)
                let rowReader = RowReader(reader)
                if reader.Read()
                then return read rowReader
                else return! failwith "Expected at least one row to be returned from the result set. Instead it was empty"
            finally
                if props.ExistingConnection.IsNone
                then connection.Dispose()
        }

    /// <summary>Executes the query and runs each row through the input lambda</summary>
    let iter (read: RowReader -> unit) (props: SqlProps) : unit =
        if props.SqlQuery.IsNone then failwith "No query provided to execute. Please use Sql.query"
        let connection = getConnection props
        try
            if not (connection.State.HasFlag ConnectionState.Open)
            then connection.Open()
            use command = new SqlCommand(props.SqlQuery.Value, connection)
            do populateCmd command props
            if props.NeedPrepare then command.Prepare()
            use reader = command.ExecuteReader()
            let rowReader = RowReader(reader)
            while reader.Read() do read rowReader
        finally
            if props.ExistingConnection.IsNone
            then connection.Dispose()

    /// <summary>Executes the query asynchronously and runs each row through the input lambda</summary>
    let iterAsync (read: RowReader -> unit) (props: SqlProps) : Task =
        unitTask {
            let! token =  Async.CancellationToken
            use mergedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token, props.CancellationToken)
            let mergedToken = mergedTokenSource.Token
            if props.SqlQuery.IsNone then failwith "No query provided to execute. Please use Sql.query"
            let connection = getConnection props
            try
                if not (connection.State.HasFlag ConnectionState.Open)
                then do! connection.OpenAsync(mergedToken)
                use command = new SqlCommand(props.SqlQuery.Value, connection)
                do populateCmd command props
                if props.NeedPrepare then command.Prepare()
                use! reader = command.ExecuteReaderAsync(mergedToken)
                let rowReader = RowReader(reader)
                while reader.Read() do read rowReader
            finally
                if props.ExistingConnection.IsNone
                then connection.Dispose()
        }

    /// <summary>Executes the query asynchronously and maps each row using the input lambda to return a final list of the each mapped value</summary>
    let executeAsync (read: RowReader -> 't) (props: SqlProps) : Task<'t list> =
        task {
            let! token =  Async.CancellationToken
            use mergedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token, props.CancellationToken)
            let mergedToken = mergedTokenSource.Token
            if props.SqlQuery.IsNone then failwith "No query provided to execute. Please use Sql.query"
            let connection = getConnection props
            try
                if not (connection.State.HasFlag ConnectionState.Open)
                then do! connection.OpenAsync(mergedToken)
                use command = new SqlCommand(props.SqlQuery.Value, connection)
                do populateCmd command props
                if props.NeedPrepare then command.Prepare()
                use! reader = command.ExecuteReaderAsync(mergedToken)
                let rowReader = RowReader(reader)
                let result = ResizeArray<'t>()
                while reader.Read() do result.Add (read rowReader)
                return List.ofSeq result
            finally
                if props.ExistingConnection.IsNone
                then connection.Dispose()
        }

    /// <summary>Executes the query and returns the number of rows affected</summary>
    let executeNonQuery (props: SqlProps) : int =
        if props.SqlQuery.IsNone then failwith "No query provided to execute..."
        let connection = getConnection props
        try
            if not (connection.State.HasFlag ConnectionState.Open)
            then connection.Open()
            use command = new SqlCommand(props.SqlQuery.Value, connection)
            populateCmd command props
            if props.NeedPrepare then command.Prepare()
            command.ExecuteNonQuery()
        finally
            if props.ExistingConnection.IsNone
            then connection.Dispose()

    /// <summary>Executes the query asynchronously and returns the number of rows affected</summary>
    let executeNonQueryAsync  (props: SqlProps) =
        task {
            let! token = Async.CancellationToken
            use mergedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token, props.CancellationToken)
            let mergedToken = mergedTokenSource.Token
            if props.SqlQuery.IsNone then failwith "No query provided to execute. Please use Sql.query"
            let connection = getConnection props
            try
                if not (connection.State.HasFlag ConnectionState.Open)
                then do! connection.OpenAsync(mergedToken)
                use command = new SqlCommand(props.SqlQuery.Value, connection)
                populateCmd command props
                if props.NeedPrepare then command.Prepare()
                let! affectedRows = command.ExecuteNonQueryAsync(mergedToken)
                return Ok affectedRows
            finally
                if props.ExistingConnection.IsNone
                then connection.Dispose()
        }
