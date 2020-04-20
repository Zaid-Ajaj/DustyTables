module Tests

open System
open System.Data
open Expecto
open DustyTables
open DustyTables.OptionWorkflow
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

    testDatabase "Reading a simple query" <| fun dustyTablesDb ->
        dustyTablesDb
        |> Sql.connect
        |> Sql.query "select * from (values (1, N'john')) as users(id, username)"
        |> Sql.executeTable
        |> Sql.mapEachRow (fun row ->
            option {
                let! id = Sql.readInt "id" row
                let! username = Sql.readString "username" row
                return (id, username)
            })
        |> function
           | [ (1, "john") ] -> pass()
           | otherwise -> fail()

    testDatabase "Reading a simple query using reader" <| fun dustyTablesDb ->
        dustyTablesDb
        |> Sql.connect
        |> Sql.query "select * from (values (1, N'john')) as users(id, username)"
        |> Sql.executeReader (fun reader ->
            let row = Sql.readRow reader
            option {
                let! id = Sql.readInt "id" row
                let! username = Sql.readString "username" row
                return (id, username)
            })
        |> function
           | [ (1, "john") ] -> pass()
           | otherwise -> fail()

    testDatabase "Reading a parameterized query" <| fun dustyTablesDb ->
        dustyTablesDb
        |> Sql.connect
        |> Sql.query "select * from (values (@userId, @username)) as users(id, username)"
        |> Sql.parameters [ "@userId", SqlValue.Int 5; "@username", SqlValue.String "jane" ]
        |> Sql.executeTable
        |> Sql.mapEachRow (fun row ->
            option {
                let! id = Sql.readInt "id" row
                let! username = Sql.readString "username" row
                return (id, username)
            })
        |> function
           | [ (5, "jane") ] -> pass()
           | otherwise -> fail()

    testDatabase "Reading a scalar query" <| fun dustyTablesDb ->
        dustyTablesDb
        |> Sql.connect
        |> Sql.query "select 1 as id"
        |> Sql.executeScalar
        |> Sql.toInt
        |> function
           | 1 -> pass()
           | n -> fail()

    testDatabase "Reading date time as scalar" <| fun dustyTablesDb ->
        dustyTablesDb
        |> Sql.connect
        |> Sql.query "select getdate()"
        |> Sql.executeScalar
        |> function
           | SqlValue.DateTime time -> pass()
           | otherwise -> fail()

    testDatabase "Sql.executeScalarSafe catches exceptions" <| fun dustyTablesDb ->
        dustyTablesDb
        |> Sql.connect
        |> Sql.query "select some invalid SQL"
        |> Sql.executeScalarSafe
        |> function
           | Error ex -> pass()
           | Ok _ -> fail()

    testDatabase "NULL values are skipped when using option workflow" <| fun dustyTablesDb ->
        dustyTablesDb
        |> Sql.connect
        |> Sql.query "select * from (values (1, NULL), (2, N'john')) as users(id, username)"
        |> Sql.executeTable
        |> Sql.mapEachRow (fun row ->
            option {
              let! id = Sql.readInt "id" row
              let! username = Sql.readString "username" row
              return (id, username)
            })
        |> function
           | [ (2, "john") ] -> pass()
           | otherwise -> fail()

    testDatabase "Executing stored procedure" <| fun dustyTablesDb ->
        dustyTablesDb
        |> Sql.connect
        |> Sql.storedProcedure "sp_executesql"
        |> Sql.parameters [ "@stmt", SqlValue.String "SELECT 42 AS A" ]
        |> Sql.executeScalar
        |> function
           | SqlValue.Int 42 -> pass()
           | otherwise -> fail()

    testDatabase "Executing parameterized function in query" <| fun dustyTablesDb ->
        dustyTablesDb
        |> Sql.connect
        |> Sql.query "SELECT LOWER(@inputText)"
        |> Sql.parameters [ "@inputText", SqlValue.String "TEXT" ]
        |> Sql.executeScalar
        |> function
           | SqlValue.String "text" -> pass()
           | otherwise -> fail()

    testDatabase "Reading different number formats" <| fun dustyTablesDb ->
        dustyTablesDb
        |> Sql.connect
        |> Sql.queryStatements [
            "select *"
            "from (values(cast(1 as tinyint), cast(1 as smallint), cast(1 as bigint)))"
            "as numbers(tiny, small, big)"
          ]
        |> Sql.executeTable
        |> Sql.mapEachRow (fun row ->
              option {
                  let! tiny = Sql.readTinyInt "tiny" row
                  let! small = Sql.readSmallInt "small" row
                  let! big = Sql.readBigInt "big" row
                  return (tiny, small, big)
              })
        |> function
           | [ tiny, small, big ] ->
              Expect.equal tiny (uint8 1) "Tiny is correct"
              Expect.equal small (int16 1) "Small is correct"
              Expect.equal big (int64 1) "big is correct"
           | otherwise ->
              fail()

    testDatabase "number formats roundtrip" <| fun dustyTablesDb ->
        dustyTablesDb
        |> Sql.connect
        |> Sql.query "select * from (values(@xs, @s, @l)) as numbers(xs, s, l)"
        |> Sql.parameters [
              "@xs", SqlValue.TinyInt (uint8 200)
              "@s", SqlValue.Smallint (int16 42)
              "@l", SqlValue.Bigint (int64 1) ]
        |> Sql.executeTable
        |> Sql.mapEachRow (fun row ->
              option {
                  let! tiny = Sql.readTinyInt "xs" row
                  let! small = Sql.readSmallInt "s" row
                  let! big = Sql.readBigInt "l" row
                  return (tiny, small, big)
              })
        |> function
          [ tiny, small, big ] ->
              Expect.equal tiny (uint8 200) "Tiny is correct"
              Expect.equal small (int16 42) "Small is correct"
              Expect.equal big (int64 1) "big is correct"
           | otherwise ->
              fail()

    testDatabase "binary roundtrip" <| fun dustyTablesDb ->
        dustyTablesDb
        |> Sql.connect
        |> Sql.query "select @blob"
        |> Sql.parameters [ "@blob", SqlValue.Binary [| byte 1; byte 2; byte 3 |] ]
        |> Sql.executeScalar
        |> function
           | SqlValue.Binary bytes -> Expect.equal bytes [| byte 1; byte 2; byte 3 |] "bytes are the same"
           | otherwise -> fail()

    testDatabase "reading unique identifier works" <| fun dustyTablesDb ->
        dustyTablesDb
        |> Sql.connect
        |> Sql.query "select newid()"
        |> Sql.executeScalar
        |> function
              | SqlValue.UniqueIdentifier value -> pass()
              | otherwise -> fail()

    testDatabase "unique identifier roundtrip" <| fun dustyTablesDb ->
        let original = Guid.NewGuid()
        dustyTablesDb
        |> Sql.connect
        |> Sql.query "select @identifier"
        |> Sql.parameters [ "@identifier", SqlValue.UniqueIdentifier original ]
        |> Sql.executeScalar
        |> function
              | SqlValue.UniqueIdentifier roundtripped -> Expect.equal original roundtripped "Roundtrip works"
              | otherwise -> fail()

    testDatabase "reading money as decimal" <| fun dustyTablesDb ->
        dustyTablesDb
        |> Sql.connect
        |> Sql.query "select cast(1.2345 as money)"
        |> Sql.executeScalar
        |> Sql.toDecimal
        |> fun value -> Expect.equal value 1.2345M "decimal is correct"

    testDatabase "Simpy reading a single column from table" <| fun dustyTablesDb ->
        dustyTablesDb
        |> Sql.connect
        |> Sql.query "select name from (values(N'one'), (N'two'), (N'three')) as numbers(name)"
        |> Sql.executeTable
        |> Sql.mapEachRow (Sql.readString "name")
        |> function
           | [ "one"; "two"; "three" ] -> pass()
           | otherwise -> fail()

    testDatabase "reading count as integer" <| fun dustyTablesDb ->
        dustyTablesDb
        |> Sql.connect
        |> Sql.query "select count(*) from (values(1, 2)) as numbers(one, two)"
        |> Sql.executeScalar
        |> Sql.toInt
        |> function
            | 1 -> pass()
            | _ -> fail()

    testDatabase "decimal roundtrip" <| fun dustyTablesDb ->
        dustyTablesDb
        |> Sql.connect
        |> Sql.query "select @value"
        |> Sql.parameters [ "@value", SqlValue.Decimal 1.234567M ]
        |> Sql.executeScalar
        |> Sql.toDecimal
        |> fun value -> Expect.equal value 1.234567M "decimal is correct"

    testDatabase "table-valued parameters in a stored procedure" <| fun dustyTablesDb ->
        // create a custom SQL type
        dustyTablesDb
        |> Sql.connect
        |> Sql.query "create type CustomPeopleTableType as table (firstName nvarchar(max), lastName nvarchar(max))"
        |> Sql.executeNonQuery
        |> ignore

        // create a stored procedure to use the custom SQL type
        dustyTablesDb
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
        let people : SqlValue =
            let customTypeName = "CustomPeopleTableType"
            let table = new DataTable()
            table.Columns.Add "firstName" |> ignore
            table.Columns.Add "lastName"  |> ignore
            table.Rows.Add("John", "Doe") |> ignore
            table.Rows.Add("Jane", "Doe") |> ignore
            table.Rows.Add("Fred", "Doe") |> ignore
            SqlValue.Table (customTypeName, table)

        // query the procedure
        dustyTablesDb
        |> Sql.connect
        |> Sql.storedProcedure "sp_TableValuedParametersTest"
        |> Sql.parameters ["@people", people]
        |> Sql.executeTable
        |> Sql.mapEachRow (fun row ->
              option {
                  let! firstName = Sql.readString "firstName" row
                  let! lastName = Sql.readString "lastName" row

                  return sprintf "%s %s" firstName lastName
              })
        |> function
           [ first; second; third ] ->
              Expect.equal first "John Doe" "First name is John Doe"
              Expect.equal second "Jane Doe" "Second name is Jane Doe"
              Expect.equal third "Fred Doe" "Third name is Fred Doe"
           | otherwise ->
              fail()
  ]
