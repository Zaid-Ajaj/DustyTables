# DustyTables [![Nuget](https://img.shields.io/nuget/v/DustyTables.svg?colorB=green)](https://www.nuget.org/packages/DustyTables)

Functional wrapper around plain old (dusty?) `SqlClient` to simplify data access when talking to MS Sql Server databases.

## Install
```bash
# nuget client
dotnet add package DustyTables
```

## Query a table
```fs
open DustyTables

// get the connection from the environment
let connectionString() = Env.getVar "app_db"

type User = { Id: int; Username: string }

let getUsers() : User list =
    connectionString()
    |> Sql.connect
    |> Sql.query "SELECT * FROM dbo.[Users]"
    |> Sql.execute (fun read ->
        {
            Id = read.int "user_id"
            Username = read.string "username"
        })
```

## Handle null values from table columns:
```fs
open DustyTables

// get the connection from the environment
let connectionString() = Env.getVar "app_db"

type User = { Id: int; Username: string; LastModified : Option<DateTime> }

let getUsers() : User list =
    connectionString()
    |> Sql.connect
    |> Sql.query "SELECT * FROM dbo.[users]"
    |> Sql.execute(fun read ->
        {
            Id = read.int "user_id"
            Username = read.string "username"
            // Notice here using `orNone` reader variants
            LastModified = read.dateTimeOrNone "last_modified"
        })
```
## Providing default values for null columns:
```fs
open DustyTables

// get the connection from the environment
let connectionString() = Env.getVar "app_db"

type User = { Id: int; Username: string; Biography : string }

let getUsers() : User list =
    connectionString()
    |> Sql.connect
    |> Sql.query "select * from dbo.[users]"
    |> Sql.execute (fun read ->
        {
            Id = read.int "user_id";
            Username = read.string "username"
            Biography = defaultArg (read.stringOrNone "bio") ""
        })
```
## Execute a parameterized query
```fs
open DustyTables

// get the connection from the environment
let connectionString() = Env.getVar "app_db"

// get product names by category
let productsByCategory (category: string) =
    connectionString()
    |> Sql.connect
    |> Sql.query "SELECT name FROM dbo.[Products] where category = @category"
    |> Sql.parameters [ "@category", Sql.string category ]
    |> Sql.execute (fun read -> read.string "name")
```
### Executing a stored procedure with parameters
```fs
open DustyTables

// get the connection from the environment
let connectionString() = Env.getVar "app_db"

// check whether a user exists or not
let userExists (username: string) : Async<bool> =
    connectionString()
    |> Sql.connect
    |> Sql.storedProcedure "user_exists"
    |> Sql.parameters [ "@username", Sql.string username ]
    |> Sql.executeRow (fun read -> read.bool 0)
```
### Executing a stored procedure with table-valued parameters
```fs
open DustyTables
open System.Data

// get the connection from the environment
let connectionString() = Env.getVar "app_db"

let executeMyStoredProcedure () : Async<int> =
    // create a table-valued parameter
    let customSqlTypeName = "MyCustomSqlTypeName"
    let dataTable = new DataTable()
    dataTable.Columns.Add "FirstName" |> ignore
    dataTable.Columns.Add "LastName"  |> ignore
    // add rows to the table parameter
    dataTable.Rows.Add("John", "Doe") |> ignore
    dataTable.Rows.Add("Jane", "Doe") |> ignore
    dataTable.Rows.Add("Fred", "Doe") |> ignore

    connectionString()
    |> Sql.connect
    |> Sql.storedProcedure "my_stored_proc"
    |> Sql.parameters
        [ "@foo", Sql.int 1
          "@people", Sql.table (customSqlTypeName, dataTable) ]
    |> Sql.executeNonQueryAsync
```

## Building and running tests locally

You only need a working local SQL server. The tests will create databases when required and dispose of them at the end of the each test

```bash
cd ./DustyTables.Build

# Build the solution
dotent run
# Run the tests
dotent run -- test
# Publish the nuget
dotnet run -- publish
```
