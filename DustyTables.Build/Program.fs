open System
open System.IO
open Fake.IO
open Fake.Core
open Spectre.Console

let banner content =
    let figlet = FigletText(content).LeftAligned()
    AnsiConsole.Render(figlet)

let path xs = Path.Combine(Array.ofList xs)

let solutionRoot = Files.findParent __SOURCE_DIRECTORY__ "DustyTables.sln";

let dustyTables = path [ solutionRoot; "DustyTables" ]
let dustyTablesTests = path [ solutionRoot; "DustyTables.Tests" ]

let test() =
    banner "Test"
    if Shell.Exec(Tools.dotnet, "run", dustyTablesTests) <> 0
    then failwith "tests failed"

let build() =
    banner "Build"
    if Shell.Exec(Tools.dotnet, "build -c Release", solutionRoot) <> 0
    then failwith "tests failed"

let publish() =
    banner "Publish"
    Shell.deleteDir (path [ dustyTables; "bin" ])
    Shell.deleteDir (path [ dustyTables; "obj" ])

    if Shell.Exec(Tools.dotnet, "pack --configuration Release", dustyTables) <> 0 then
        failwith "Pack failed"
    else
        let nugetKey =
            match Environment.environVarOrNone "NUGET_KEY" with
            | Some nugetKey -> nugetKey
            | None -> failwith "The Nuget API key must be set in a NUGET_KEY environmental variable"

        let nugetPath =
            Directory.GetFiles(path [ dustyTables; "bin"; "Release" ])
            |> Seq.head
            |> Path.GetFullPath

        if Shell.Exec(Tools.dotnet, sprintf "nuget push %s -s nuget.org -k %s" nugetPath nugetKey, dustyTables) <> 0
        then failwith "Publish failed"

[<EntryPoint>]
let main argv =
    try
        match argv with
        | [| |] -> build() 
        | [| "test" |] -> build(); test()
        | [| "publish" |] -> build(); test(); publish();
        | _ -> ()

        0
    with
    | ex ->
        printfn "Error occured"
        printfn "%A" ex
        1
