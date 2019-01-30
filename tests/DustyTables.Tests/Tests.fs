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
let tests =
  testList "DustyTables" [

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

    testCase "NULL values are skipped" <| fun _ ->
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
  ] 
