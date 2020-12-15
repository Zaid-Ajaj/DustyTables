namespace DustyTables

open System
open System.Threading.Tasks
open System.Data
open Microsoft.Data.SqlClient
open System.Threading
open System.Text.RegularExpressions

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
        SqlParameter(Value = value,
                     TypeName = typeName,
                     SqlDbType = SqlDbType.Structured)

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

    let executeTransactionAsync queries (props: SqlProps)  =
        async {
            try
                let! token = Async.CancellationToken
                use mergedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token, props.CancellationToken)
                let mergedToken = mergedTokenSource.Token
                if List.isEmpty queries
                then return Ok [ ]
                else
                let connection = getConnection props
                try
                    if not (connection.State.HasFlag ConnectionState.Open)
                    then do! Async.AwaitTask (connection.OpenAsync mergedToken)
                    use transaction = connection.BeginTransaction ()
                    let affectedRowsByQuery = ResizeArray<int>()
                    for (query, parameterSets) in queries do
                        if List.isEmpty parameterSets
                        then
                          use command = new SqlCommand(query, connection, transaction)
                          let! affectedRows = Async.AwaitTask (command.ExecuteNonQueryAsync mergedToken)
                          affectedRowsByQuery.Add affectedRows
                        else
                          for parameterSet in parameterSets do
                            use command = new SqlCommand(query, connection, transaction)
                            populateRow command parameterSet
                            let! affectedRows = Async.AwaitTask (command.ExecuteNonQueryAsync mergedToken)
                            affectedRowsByQuery.Add affectedRows
                    transaction.Commit()
                    return Ok (List.ofSeq affectedRowsByQuery)
                finally
                    if props.ExistingConnection.IsNone
                    then connection.Dispose()
            with error ->
                return Error error
        }

    let execute (read: RowReader -> 't) (props: SqlProps) : Result<'t list, exn> =
        try
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
                Ok (List.ofSeq result)
            finally
                if props.ExistingConnection.IsNone
                then connection.Dispose()
        with error ->
            Error error

    let executeRow (read: RowReader -> 't) (props: SqlProps) : Result<'t, exn> =
        try
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
                then Ok (read rowReader)
                else failwith "Expected at least one row to be returned from the result set. Instead it was empty"
            finally
                if props.ExistingConnection.IsNone
                then connection.Dispose()
        with error ->
            Error error

    let executeRowAsync (read: RowReader -> 't) (props: SqlProps) : Async<Result<'t, exn>> =
        async {
            try
                let! token =  Async.CancellationToken
                use mergedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token, props.CancellationToken)
                let mergedToken = mergedTokenSource.Token
                if Option.isNone props.SqlQuery then failwith "No query provided to execute. Please use Sql.query"
                let connection = getConnection props
                try
                    if not (connection.State.HasFlag ConnectionState.Open)
                    then do! Async.AwaitTask(connection.OpenAsync(mergedToken))
                    use command = new SqlCommand(props.SqlQuery.Value, connection)
                    do populateCmd command props
                    if props.NeedPrepare then command.Prepare()
                    use! reader = Async.AwaitTask (command.ExecuteReaderAsync(mergedToken))
                    let rowReader = RowReader(reader)
                    if reader.Read()
                    then return Ok (read rowReader)
                    else return! failwith "Expected at least one row to be returned from the result set. Instead it was empty"
                finally
                    if props.ExistingConnection.IsNone
                    then connection.Dispose()
            with error ->
                return Error error
        }

    let iter (read: RowReader -> unit) (props: SqlProps) : Result<unit, exn> =
        try
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
                Ok ()
            finally
                if props.ExistingConnection.IsNone
                then connection.Dispose()
        with error ->
            Error error

    let iterAsync (read: RowReader -> unit) (props: SqlProps) : Async<Result<unit, exn>> =
        async {
            try
                let! token =  Async.CancellationToken
                use mergedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token, props.CancellationToken)
                let mergedToken = mergedTokenSource.Token
                if props.SqlQuery.IsNone then failwith "No query provided to execute. Please use Sql.query"
                let connection = getConnection props
                try
                    if not (connection.State.HasFlag ConnectionState.Open)
                    then do! Async.AwaitTask(connection.OpenAsync(mergedToken))
                    use command = new SqlCommand(props.SqlQuery.Value, connection)
                    do populateCmd command props
                    if props.NeedPrepare then command.Prepare()
                    use! reader = Async.AwaitTask (command.ExecuteReaderAsync(mergedToken))
                    let rowReader = RowReader(reader)
                    while reader.Read() do read rowReader
                    return Ok ()
                finally
                    if props.ExistingConnection.IsNone
                    then connection.Dispose()
            with error ->
                return Error error
        }

    let executeAsync (read: RowReader -> 't) (props: SqlProps) : Async<Result<'t list, exn>> =
        async {
            try
                let! token =  Async.CancellationToken
                use mergedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token, props.CancellationToken)
                let mergedToken = mergedTokenSource.Token
                if props.SqlQuery.IsNone then failwith "No query provided to execute. Please use Sql.query"
                let connection = getConnection props
                try
                    if not (connection.State.HasFlag ConnectionState.Open)
                    then do! Async.AwaitTask(connection.OpenAsync(mergedToken))
                    use command = new SqlCommand(props.SqlQuery.Value, connection)
                    do populateCmd command props
                    if props.NeedPrepare then command.Prepare()
                    use! reader = Async.AwaitTask (command.ExecuteReaderAsync(mergedToken))
                    let rowReader = RowReader(reader)
                    let result = ResizeArray<'t>()
                    while reader.Read() do result.Add (read rowReader)
                    return Ok (List.ofSeq result)
                finally
                    if props.ExistingConnection.IsNone
                    then connection.Dispose()
            with error ->
                return Error error
        }

    /// Executes the query and returns the number of rows affected
    let executeNonQuery (props: SqlProps) : Result<int, exn> =
        try
            if props.SqlQuery.IsNone then failwith "No query provided to execute..."
            let connection = getConnection props
            try
                if not (connection.State.HasFlag ConnectionState.Open)
                then connection.Open()
                use command = new SqlCommand(props.SqlQuery.Value, connection)
                populateCmd command props
                if props.NeedPrepare then command.Prepare()
                Ok (command.ExecuteNonQuery())
            finally
                if props.ExistingConnection.IsNone
                then connection.Dispose()
        with
            | error -> Error error

    /// Executes the query as asynchronously and returns the number of rows affected
    let executeNonQueryAsync  (props: SqlProps) =
        async {
            try
                let! token = Async.CancellationToken
                use mergedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(token, props.CancellationToken)
                let mergedToken = mergedTokenSource.Token
                if props.SqlQuery.IsNone then failwith "No query provided to execute. Please use Sql.query"
                let connection = getConnection props
                try
                    if not (connection.State.HasFlag ConnectionState.Open)
                    then do! Async.AwaitTask (connection.OpenAsync(mergedToken))
                    use command = new SqlCommand(props.SqlQuery.Value, connection)
                    populateCmd command props
                    if props.NeedPrepare then command.Prepare()
                    let! affectedRows = Async.AwaitTask(command.ExecuteNonQueryAsync(mergedToken))
                    return Ok affectedRows
                finally
                    if props.ExistingConnection.IsNone
                    then connection.Dispose()
            with
            | error -> return Error error
        }

    type private TempTableLoader(fieldCount, items: obj seq) =
        let enumerator = items.GetEnumerator()

        interface IDataReader with
            member this.FieldCount: int = fieldCount
            member this.Read(): bool = enumerator.MoveNext()
            member this.GetValue(i: int): obj =
                let row : obj[] = unbox enumerator.Current
                row.[i]
            member this.Dispose(): unit = ()

            member __.Close(): unit = invalidOp "NotImplementedException"
            member __.Depth: int = invalidOp "NotImplementedException"
            member __.GetBoolean(_: int): bool = invalidOp "NotImplementedException"
            member __.GetByte(_ : int): byte = invalidOp "NotImplementedException"
            member __.GetBytes(_ : int, _ : int64, _ : byte [], _ : int, _ : int): int64 = invalidOp "NotImplementedException"
            member __.GetChar(_ : int): char = invalidOp "NotImplementedException"
            member __.GetChars(_ : int, _ : int64, _ : char [], _ : int, _ : int): int64 = invalidOp "NotImplementedException"
            member __.GetData(_ : int): IDataReader = invalidOp "NotImplementedException"
            member __.GetDataTypeName(_ : int): string = invalidOp "NotImplementedException"
            member __.GetDateTime(_ : int): System.DateTime = invalidOp "NotImplementedException"
            member __.GetDecimal(_ : int): decimal = invalidOp "NotImplementedException"
            member __.GetDouble(_ : int): float = invalidOp "NotImplementedException"
            member __.GetFieldType(_ : int): System.Type = invalidOp "NotImplementedException"
            member __.GetFloat(_ : int): float32 = invalidOp "NotImplementedException"
            member __.GetGuid(_ : int): System.Guid = invalidOp "NotImplementedException"
            member __.GetInt16(_ : int): int16 = invalidOp "NotImplementedException"
            member __.GetInt32(_ : int): int = invalidOp "NotImplementedException"
            member __.GetInt64(_ : int): int64 = invalidOp "NotImplementedException"
            member __.GetName(_ : int): string = invalidOp "NotImplementedException"
            member __.GetOrdinal(_ : string): int = invalidOp "NotImplementedException"
            member __.GetSchemaTable(): DataTable = invalidOp "NotImplementedException"
            member __.GetString(_ : int): string = invalidOp "NotImplementedException"
            member __.GetValues(_ : obj []): int = invalidOp "NotImplementedException"
            member __.IsClosed: bool = invalidOp "NotImplementedException"
            member __.IsDBNull(_ : int): bool = invalidOp "NotImplementedException"
            member __.Item with get (_ : int): obj = invalidOp "NotImplementedException"
            member __.Item with get (_ : string): obj = invalidOp "NotImplementedException"
            member __.NextResult(): bool = invalidOp "NotImplementedException"
            member __.RecordsAffected: int = invalidOp "NotImplementedException"

    type TempTable = 
        { Name : string 
          Columns : Map<string, int> }

    let private tempTableNameRegex = Regex("(#[a-z0-9\\-_]+)", RegexOptions.IgnoreCase)

    let private tempTableColumnRegex = 
        [ "bigint"
          "binary"
          "bit"
          "char"
          "datetimeoffset"
          "datetime2"
          "datetime"
          "date"
          "decimal"
          "float"
          "image"
          "int"
          "nchar"
          "ntext"
          "nvarchar"
          "real"
          "timestamp"
          "varbinary" ]
        |> String.concat "|"
        |> fun x -> Regex(@"[\[]{0,1}([a-z0-9\-_]+)[\]]{0,1} (?:"+x+")", RegexOptions.IgnoreCase)

    let createTempTable table (props : SqlProps) = 
        let connection = getConnection props
        if not (connection.State.HasFlag ConnectionState.Open) then connection.Open()

        use command = new SqlCommand(table, connection)
        command.ExecuteNonQuery() |> ignore

        let name = tempTableNameRegex.Match(table).Groups.[1].Value

        let columns = 
            tempTableColumnRegex.Matches(table)
            |> Seq.cast
            |> Seq.mapi(fun i (m : Match) -> m.Groups.[1].Value, i )
            |> Map.ofSeq

        let info =
            { TempTable.Name = name
              Columns = columns }
        
        { props with ExistingConnection = Some connection }, info

    let tempTableData data (props, info : TempTable) =
        props, info, data

    let loadTempTable mapper (props : SqlProps, info : TempTable, data) =
        let items =
            data
            |> Seq.map(fun item -> 
                let cols = mapper item

                let arr = Array.zeroCreate info.Columns.Count
                cols
                |> List.iter(fun (name, p : SqlParameter) -> 
                    let index = info.Columns |> Map.find name
                    arr.[index] <- p.Value
                )
                box arr
            )

        use reader = new TempTableLoader(info.Columns.Count, items)

        use bulkCopy = new SqlBulkCopy(props.ExistingConnection.Value)
        props.Timeout |> Option.iter (fun x -> bulkCopy.BulkCopyTimeout <- x)
        bulkCopy.BatchSize <- 5000
        bulkCopy.DestinationTableName <- info.Name
        bulkCopy.WriteToServer(reader)

        props

    let executeStream (read: RowReader -> 't) (props : SqlProps) =
        seq {
            if props.SqlQuery.IsNone then failwith "No query provided to execute. Please use Sql.query"
            let connection = getConnection props
            try
                if not (connection.State.HasFlag ConnectionState.Open)
                then connection.Open()
                use command = new SqlCommand(props.SqlQuery.Value, connection)
                props.Timeout |> Option.iter (fun x -> command.CommandTimeout <- x)
                do populateCmd command props
                if props.NeedPrepare then command.Prepare()
                use reader = command.ExecuteReader()
                let rowReader = RowReader(reader)
                while reader.Read() do
                    read rowReader
            finally
                if props.ExistingConnection.IsNone
                then connection.Dispose()
        }
