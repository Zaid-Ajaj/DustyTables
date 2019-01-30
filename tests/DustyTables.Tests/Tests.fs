module Tests

open System
open Expecto
open DustyTables
open DustyTables.OptionWorkflow

let dustyTablesDb = Environment.GetEnvironmentVariable("DUSTY_TABLES_DB", EnvironmentVariableTarget.User)
if isNull dustyTablesDb then failwith "Missing required environment variable DUSTY_TABLE_DB that points to database"

let pass() = Expect.isTrue true "true is true :)"
let fail() = Expect.isTrue false "true is false :("

[<Tests>]
let tests = testList "DustyTables" [

      testCase "Reading a simple query" <| fun _ ->
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

      testCase "Reading a parameterized query" <| fun _ ->
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

      testCase "Reading a scalar query" <| fun _ ->
          dustyTablesDb
          |> Sql.connect
          |> Sql.query "select 1 as id"
          |> Sql.executeScalar
          |> Sql.toInt 
          |> function 
             | 1 -> pass()
             | n -> fail()

      testCase "Reading date time as scalar" <| fun _ ->
          dustyTablesDb
          |> Sql.connect
          |> Sql.query "select getdate()"
          |> Sql.executeScalar 
          |> function 
             | SqlValue.DateTime time -> pass()
             | otherwise -> fail()

      testCase "Sql.executeScalarSafe catches exceptions" <| fun _ ->
          dustyTablesDb
          |> Sql.connect
          |> Sql.query "select some invalid SQL"
          |> Sql.executeScalarSafe
          |> function
             | Error ex -> pass()
             | Ok _ -> fail()

      testCase "NULL values are skipped when using option workflow" <| fun _ ->
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

      testCase "Executing stored procedure" <| fun _ ->
          dustyTablesDb
          |> Sql.connect
          |> Sql.storedProcedure "sp_executesql"
          |> Sql.parameters [ "@stmt", SqlValue.String "SELECT 42 AS A" ]
          |> Sql.executeScalar
          |> function
             | SqlValue.Int 42 -> pass()
             | otherwise -> fail()

      testCase "Executing parameterized function in query" <| fun _ ->
          dustyTablesDb
          |> Sql.connect
          |> Sql.query "SELECT LOWER(@inputText)"
          |> Sql.parameters [ "@inputText", SqlValue.String "TEXT" ]
          |> Sql.executeScalar
          |> function
             | SqlValue.String "text" -> pass()
             | otherwise -> fail()

      testCase "Reading different number formats" <| fun _ ->
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

      testCase "number formats roundtrip" <| fun _ ->
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

      testCase "binary roundtrip" <| fun _ ->
          dustyTablesDb
          |> Sql.connect
          |> Sql.query "select @blob"
          |> Sql.parameters [ "@blob", SqlValue.Binary [| byte 1; byte 2; byte 3 |] ]
          |> Sql.executeScalar
          |> function
             | SqlValue.Binary bytes -> Expect.equal bytes [| byte 1; byte 2; byte 3 |] "bytes are the same"
             | otherwise -> fail()

      testCase "reading money as decimal" <| fun _ ->
          dustyTablesDb
          |> Sql.connect
          |> Sql.query "select cast(1.2345 as money)"
          |> Sql.executeScalar
          |> Sql.toDecimal
          |> fun value -> Expect.equal value 1.2345M "decimal is correct"

      testCase "Simpy reading a single column from table" <| fun _ ->
          dustyTablesDb
          |> Sql.connect
          |> Sql.query "select name from (values(N'one'), (N'two'), (N'three')) as numbers(name)"
          |> Sql.executeTable
          |> Sql.mapEachRow (Sql.readString "name")
          |> function
             | [ "one"; "two"; "three" ] -> pass()
             | otherwise -> fail()

      testCase "reading count as integer" <| fun _ ->
          dustyTablesDb
          |> Sql.connect
          |> Sql.query "select count(*) from (values(1, 2)) as numbers(one, two)"
          |> Sql.executeScalar
          |> Sql.toInt
          |> function
              | 1 -> pass()
              | _ -> fail()

      testCase "decimal roundtrip" <| fun _ ->
          dustyTablesDb
          |> Sql.connect
          |> Sql.query "select @value"
          |> Sql.parameters [ "@value", SqlValue.Decimal 1.234567M ]
          |> Sql.executeScalar
          |> Sql.toDecimal
          |> fun value -> Expect.equal value 1.234567M "decimal is correct"
  ] 
