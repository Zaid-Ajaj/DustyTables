# DustyTables

Functional wrapper around plain old (dusty?) `SqlClient` to simplify data access when talking to MS Sql Server databases. 

## Install
```bash
# nuget client
dotnet add package DustyTables

# or using paket
.paket/paket.exe add DustyTables --project path/to/project.fsproj
```

## Query a table
```fs
open DustyTables
open DustyTables.OptionWorkflow

// get the connection from the environment
let connectionString() = Env.getVar "app_db"

type User = { Id: int; Username: string }

let getUsers() : User list = 
    connectionString()
    |> Sql.connect
    |> Sql.query "select * from dbo.[users]"
    |> Sql.executeTable 
    |> Sql.mapEachRow (fun row -> 
        option {
            let! id = Sql.readInt "user_id" row
            let! username = Sql.readString "username" row
            return { Id = id; Username = username }
        })
```
Notice that we are using the `option` workflow which means if any row has "user_id" or "username" as NULL it will be skipped

## Handle null values from table columns:
```fs
open DustyTables
open DustyTables.OptionWorkflow

// get the connection from the environment
let connectionString() = Env.getVar "app_db"

type User = { Id: int; Username: string; LastModified : Option<DateTime> }

let getUsers() : User list = 
    connectionString()
    |> Sql.connect
    |> Sql.query "select * from dbo.[users]"
    |> Sql.executeTable 
    |> Sql.mapEachRow (fun row -> 
        option {
            let! id = Sql.readInt "user_id" row
            let! username = Sql.readString "username" row
            // using "let" instead of "let!"
            let lastModified = Sql.readDateTime "last_modified" row
            return { 
                Id = id; 
                Username = username
                LastModified = lastModified  
            }
        })
```
## Providing default values for null columns:
```fs
open DustyTables
open DustyTables.OptionWorkflow

// get the connection from the environment
let connectionString() = Env.getVar "app_db"

type User = { Id: int; Username: string; Biography : string }

let getUsers() : User list = 
    connectionString()
    |> Sql.connect
    |> Sql.query "select * from dbo.[users]"
    |> Sql.executeTable 
    |> Sql.mapEachRow (fun row -> 
        option {
            let! id = Sql.readInt "user_id" row
            let! username = Sql.readString "username" row
            let userBiography = Sql.readString "bio" row
            return { 
                Id = id; 
                Username = username
                Biography = defaultArg userBiography ""
            }
        })
```
## Query a scalar value safely:
```fs
open DustyTables
open DustyTables.OptionWorkflow

// get the connection from the environment
let connectionString() = Env.getVar "app_db"

let pingDatabase() : Option<DateTime> = 
    connectionString()
    |> Sql.connect
    |> Sql.query "select getdate()"
    |> Sql.executeScalarSafe 
    |> function 
        | Ok (SqlValue.DateTime time) -> Some time
        | otherwise -> None
```
### Query a scalar value asynchronously
```fs
open DustyTables
open DustyTables.OptionWorkflow

// get the connection from the environment
let connectionString() = Env.getVar "app_db"

let pingDatabase() : Async<Option<DateTime>> = 
    async {
        let! serverTime = 
            connectionString()
            |> Sql.connect
            |> Sql.query "select getdate()"
            |> Sql.executeScalarSafeAsync
        
        match serverTime with 
        | Ok (SqlValue.DateTime time) -> return Some time
        | otherwise -> return None
    }
```
## Execute a parameterized query
```fs
open DustyTables
open DustyTables.OptionWorkflow

// get the connection from the environment
let connectionString() = Env.getVar "app_db"

// get product names by category
let productsByCategory (category: string) : string list = 
    connectionString()
    |> Sql.connect
    |> Sql.query "select name from dbo.[products] where category = @category"
    |> Sql.parameters [ "@category", SqlValue.String category ]
    |> Sql.executeTable
    |> Sql.mapEachRow (Sql.readString "name")
```
### Executing a stored procedure with parameters
```fs
open DustyTables

// get the connection from the environment
let connectionString() = Env.getVar "app_db"

// check whether a user exists or not
let userExists (username: string) : Async<bool> = 
    async {
        let! userExists = 
            connectionString()
            |> Sql.connect
            |> Sql.storedProcedure "user_exists"
            |> Sql.parameters [ "@username", SqlValue.String username ]
            |> Sql.executeScalarAsync 
        
        return Sql.toBool userExists 
    }
```

## Running Tests locally

You only need a connection string to a working database, no tables/stored procedures/anything is requires. Just set environment variable `DUSTY_TABLES_DB` To your connection string and run the tests


## Builds

MacOS/Linux | Windows
--- | ---
[![Travis Badge](https://travis-ci.org/zaid-ajaj/DustyTables.svg?branch=master)](https://travis-ci.org/zaid-ajaj/DustyTables) | [![Build status](https://ci.appveyor.com/api/projects/status/github/zaid-ajaj/DustyTables?svg=true)](https://ci.appveyor.com/project/zaid-ajaj/DustyTables)
[![Build History](https://buildstats.info/travisci/chart/zaid-ajaj/DustyTables)](https://travis-ci.org/zaid-ajaj/DustyTables/builds) | [![Build History](https://buildstats.info/appveyor/chart/zaid-ajaj/DustyTables)](https://ci.appveyor.com/project/zaid-ajaj/DustyTables)  


## Nuget 

Stable | Prerelease
--- | ---
[![NuGet Badge](https://buildstats.info/nuget/DustyTables)](https://www.nuget.org/packages/DustyTables/) | [![NuGet Badge](https://buildstats.info/nuget/DustyTables?includePreReleases=true)](https://www.nuget.org/packages/DustyTables/)

---

### Building


Make sure the following **requirements** are installed in your system:

* [dotnet SDK](https://www.microsoft.com/net/download/core) 2.0 or higher
* [Mono](http://www.mono-project.com/) if you're on Linux or macOS.

```
> build.cmd // on windows
$ ./build.sh  // on unix
```

#### Environment Variables

* `CONFIGURATION` will set the [configuration](https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-build?tabs=netcore2x#options) of the dotnet commands.  If not set it will default to Release.
  * `CONFIGURATION=Debug ./build.sh` will result in things like `dotnet build -c Debug`
* `GITHUB_TOKEN` will be used to upload release notes and nuget packages to github.
  * Be sure to set this before releasing

### Watch Tests

The `WatchTests` target will use [dotnet-watch](https://github.com/aspnet/Docs/blob/master/aspnetcore/tutorials/dotnet-watch.md) to watch for changes in your lib or tests and re-run your tests on all `TargetFrameworks`

```
./build.sh WatchTests
```

### Releasing
* [Start a git repo with a remote](https://help.github.com/articles/adding-an-existing-project-to-github-using-the-command-line/)

```
git add .
git commit -m "Scaffold"
git remote add origin origin https://github.com/user/MyCoolNewLib.git
git push -u origin master
```

* [Add your nuget API key to paket](https://fsprojects.github.io/Paket/paket-config.html#Adding-a-NuGet-API-key)

```
paket config add-token "https://www.nuget.org" 4003d786-cc37-4004-bfdf-c4f3e8ef9b3a
```

* [Create a GitHub OAuth Token](https://help.github.com/articles/creating-a-personal-access-token-for-the-command-line/)
    * You can then set the `GITHUB_TOKEN` to upload release notes and artifacts to github
    * Otherwise it will fallback to username/password


* Then update the `RELEASE_NOTES.md` with a new version, date, and release notes [ReleaseNotesHelper](https://fsharp.github.io/FAKE/apidocs/fake-releasenoteshelper.html)

```
#### 0.2.0 - 2017-04-20
* FEATURE: Does cool stuff!
* BUGFIX: Fixes that silly oversight
```

* You can then use the `Release` target.  This will:
    * make a commit bumping the version:  `Bump version to 0.2.0` and add the release notes to the commit
    * publish the package to nuget
    * push a git tag

```
./build.sh Release
```


### Code formatting

To format code run the following target

```
./build.sh FormatCode
```

This uses [Fantomas](https://github.com/fsprojects/fantomas) to do code formatting.  Please report code formatting bugs to that repository.
