module Tests

open System
open System.Data
open Expecto
open DustyTables
open ThrowawayDb

let pass() = Expect.isTrue true "true is true :)"
let fail() = Expect.isTrue false "true is false :("

let testDatabase testName f =
    testCase testName <| fun _ ->
        use db = ThrowawayDatabase.FromLocalInstance("localhost\\SQLEXPRESS")
        f db.ConnectionString

let ftestDatabase testName f =
    ftestCase testName <| fun _ ->
        use db = ThrowawayDatabase.FromLocalInstance("localhost\\SQLEXPRESS")
        f db.ConnectionString

[<Tests>]
let tests = testList "DustyTables" [

    testDatabase "Reading a simple query" <| fun connectionString ->
        connectionString
        |> Sql.connect
        |> Sql.query "select * from (values (1, N'john')) as users(id, username)"
        |> Sql.execute (fun read -> read.int "id", read.string "username")
        |> function
           | Ok [ (1, "john") ] -> pass()
           | otherwise -> fail()

    testDatabase "Iterating over the rows works" <| fun connectionString ->
        let rows = ResizeArray<int * string>()
        connectionString
        |> Sql.connect
        |> Sql.query "select * from (values (1, N'john')) as users(id, username)"
        |> Sql.iter (fun read -> rows.Add(read.int "id", read.string "username"))
        |> function
           | Ok () ->
                Expect.equal 1 (fst rows.[0]) "Number is read correctly"
                Expect.equal "john" (snd rows.[0]) "String is read correctly"
           | otherwise ->
                fail()

    testDatabase "Reading a parameterized query" <| fun connectionString ->
        connectionString
        |> Sql.connect
        |> Sql.query "select * from (values (@userId, @username)) as users(id, username)"
        |> Sql.parameters [ "@userId", Sql.int 5; "@username", Sql.string "jane" ]
        |> Sql.execute (fun read -> read.int "id", read.string "username")
        |> function
           | Ok [ (5, "jane") ] -> pass()
           | otherwise -> fail()

    testDatabase "Reading date time as scalar" <| fun connectionString ->
        connectionString
        |> Sql.connect
        |> Sql.query "select getdate() as now"
        |> Sql.execute (fun read -> read.dateTime "now")
        |> function
           | Ok [ time ] -> pass()
           | otherwise -> fail()

    testDatabase "Sql.execute catches exceptions" <| fun connectionString ->
        connectionString
        |> Sql.connect
        |> Sql.query "select some invalid SQL"
        |> Sql.execute (fun read -> None)
        |> function
           | Error ex -> pass()
           | Ok _ -> fail()

    testDatabase "Executing stored procedure" <| fun connectionString ->
        connectionString
        |> Sql.connect
        |> Sql.storedProcedure "sp_executesql"
        |> Sql.parameters [ "@stmt", Sql.string "SELECT 42 AS A" ]
        |> Sql.execute (fun read -> read.int "A")
        |> function
           | Ok [ 42 ]-> pass()
           | Error error -> raise error
           | otherwise -> fail()

    testDatabase "Executing parameterized function in query" <| fun connectionString ->
        connectionString
        |> Sql.connect
        |> Sql.query "SELECT LOWER(@inputText) as output"
        |> Sql.parameters [ "@inputText", Sql.string "TEXT" ]
        |> Sql.execute (fun read -> read.string "output")
        |> function
           | Ok [ "text" ] -> pass()
           | otherwise -> fail()

    testDatabase "binary roundtrip" <| fun connectionString ->
        connectionString
        |> Sql.connect
        |> Sql.query "select @blob as blob"
        |> Sql.parameters [ "@blob", Sql.bytes [| byte 1; byte 2; byte 3 |] ]
        |> Sql.execute (fun read -> read.bytes "blob")
        |> function
           | Ok [ bytes ] -> Expect.equal bytes [| byte 1; byte 2; byte 3 |] "bytes are the same"
           | Error ex -> raise ex
           | otherwise -> fail()

    testDatabase "reading unique identifier works" <| fun connectionString ->
        connectionString
        |> Sql.connect
        |> Sql.query "select newid() as uid"
        |> Sql.execute (fun read -> read.uniqueidentifier "uid")
        |> function
              | Ok [ value ] -> pass()
              | Error ex -> raise ex
              | otherwise -> fail()

    testDatabase "unique identifier roundtrip" <| fun connectionString ->
        let original = Guid.NewGuid()
        connectionString
        |> Sql.connect
        |> Sql.query "select @identifier as value"
        |> Sql.parameters [ "@identifier", Sql.uniqueidentifier original ]
        |> Sql.execute (fun read -> read.uniqueidentifier "value")
        |> function
              | Ok [ roundtripped ] -> Expect.equal original roundtripped "Roundtrip works"
              | Error ex -> raise ex
              | otherwise -> fail()

    testDatabase "unique identifier roundtrip from string" <| fun connectionString ->
        let original = Guid.NewGuid()
        connectionString
        |> Sql.connect
        |> Sql.query "select @identifier as value"
        |> Sql.parameters [ "@identifier", Sql.uniqueidentifier original ]
        |> Sql.execute (fun read -> read.string "value")
        |> function
              | Ok [ roundtripped ] -> Expect.equal (string original) roundtripped "Roundtrip works"
              | Error ex -> raise ex
              | otherwise -> fail()

    testDatabase "Simpy reading a single column from table" <| fun connectionString ->
        connectionString
        |> Sql.connect
        |> Sql.query "select name from (values(N'one'), (N'two'), (N'three')) as numbers(name)"
        |> Sql.execute (fun read -> read.string "name")
        |> function
           | Ok [ "one"; "two"; "three" ] -> pass()
           | Error ex -> raise ex
           | otherwise -> fail()

    testDatabase "reading count as int64" <| fun connectionString ->
        connectionString
        |> Sql.connect
        |> Sql.query "select count(*) from (values(1, 2)) as numbers(one, two)"
        |> Sql.execute (fun read -> read.int64 0)
        |> function
            | Ok [ 1L ] -> pass()
            | Ok otherwise -> fail()
            | Error ex -> raise ex

    testDatabase "reading count as int32" <| fun connectionString ->
        connectionString
        |> Sql.connect
        |> Sql.query "select count(*) from (values(1, 2)) as numbers(one, two)"
        |> Sql.execute (fun read -> read.int 0)
        |> function
            | Ok [ 1 ] -> pass()
            | Ok otherwise -> fail()
            | Error ex -> raise ex

    testDatabase "decimal roundtrip" <| fun connectionString ->
        connectionString
        |> Sql.connect
        |> Sql.query "select @value as value"
        |> Sql.parameters [ "@value", Sql.decimal 1.234567M ]
        |> Sql.execute (fun read -> read.decimal "value")
        |> function
            | Ok [ value ] -> Expect.equal value 1.234567M "decimal is correct"
            | Error ex -> raise ex
            | otherwise -> fail()

    testDatabase "reading double as decimal" <| fun connectionString ->
        connectionString
        |> Sql.connect
        |> Sql.query "select @value as value"
        |> Sql.parameters [ "@value", Sql.double 1.2345 ]
        |> Sql.execute (fun read -> read.decimal "value")
        |> function
            | Ok [ value ] -> Expect.equal value 1.2345M "decimal is correct"
            | Error ex -> raise ex
            | otherwise -> fail()

    testDatabase "reading tinyint as int" <| fun connectionString ->
        connectionString
        |> Sql.connect
        |> Sql.query "SELECT CAST(@value as tinyint) as output"
        |> Sql.parameters [ "value", Sql.int 1 ]
        |> Sql.execute (fun read -> read.int "output")
        |> function
            | Ok [ 1 ] -> pass()
            | Error error -> raise error
            | otherwise -> fail()

    testDatabase "reading tinyint as int64" <| fun connectionString ->
        connectionString
        |> Sql.connect
        |> Sql.query "SELECT CAST(@value as tinyint) as output"
        |> Sql.parameters [ "value", Sql.int 1 ]
        |> Sql.execute (fun read -> read.int64 "output")
        |> function
            | Ok [ 1L ] -> pass()
            | Error error -> raise error
            | otherwise -> fail()

    testDatabase "table-valued parameters in a stored procedure" <| fun connectionString ->
        // create a custom SQL type
        connectionString
        |> Sql.connect
        |> Sql.query "create type CustomPeopleTableType as table (firstName nvarchar(max), lastName nvarchar(max))"
        |> Sql.executeNonQuery
        |> ignore

        // create a stored procedure to use the custom SQL type
        connectionString
        |> Sql.connect
        |> Sql.query """
            create proc sp_TableValuedParametersTest
                (@people CustomPeopleTableType READONLY)
            as
            begin
                select firstName, lastName from @people
            end
        """
        |> Sql.executeNonQuery
        |> ignore

        // create a new table-valued parameter
        let people =
            let customTypeName = "CustomPeopleTableType"
            let table = new DataTable()
            table.Columns.Add "firstName" |> ignore
            table.Columns.Add "lastName"  |> ignore
            table.Rows.Add("John", "Doe") |> ignore
            table.Rows.Add("Jane", "Doe") |> ignore
            table.Rows.Add("Fred", "Doe") |> ignore
            Sql.table (customTypeName, table)

        // query the procedure
        connectionString
        |> Sql.connect
        |> Sql.storedProcedure "sp_TableValuedParametersTest"
        |> Sql.parameters ["@people", people]
        |> Sql.execute (fun read -> read.string "firstName" + " " + read.string "lastName")
        |> function
           | Ok [ first; second; third ] ->
              Expect.equal first "John Doe" "First name is John Doe"
              Expect.equal second "Jane Doe" "Second name is Jane Doe"
              Expect.equal third "Fred Doe" "Third name is Fred Doe"
           | Error ex -> raise ex
           | otherwise ->
              fail()
  ]
