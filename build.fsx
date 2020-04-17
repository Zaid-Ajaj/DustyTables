#load ".fake/build.fsx/intellisense.fsx"
#if !FAKE
#r "Facades/netstandard"
#r "netstandard"
#endif
open System
open Fake.SystemHelper
open Fake.Core
open Fake.DotNet
open Fake.Tools
open Fake.IO
open Fake.IO.FileSystemOperators
open Fake.IO.Globbing.Operators
open Fake.Core.TargetOperators
open Fake.Api
open Fake.BuildServer

BuildServer.install [
    AppVeyor.Installer
    Travis.Installer
]

let run fileName args workingDir =
    printfn "CWD: %s" workingDir
    let fileName, args =
        if Environment.isUnix
        then fileName, args else "cmd", ("/C " + fileName + " " + args)

    if Shell.Exec(fileName, args, workingDir) <> 0
    then failwithf "Command %s %s at '%s' failed" fileName args workingDir

let release = Fake.Core.ReleaseNotes.load "RELEASE_NOTES.md"
let productName = "DustyTables"
let sln = "DustyTables.sln"
let srcGlob =__SOURCE_DIRECTORY__  @@ "src/**/*.??proj"
let testsGlob = __SOURCE_DIRECTORY__  @@ "tests/**/*.??proj"

let srcAndTest =
    !! srcGlob
    ++ testsGlob

let src = __SOURCE_DIRECTORY__ </> "src" </> "DustyTables"
let tests = __SOURCE_DIRECTORY__ </> "tests" </> "DustyTables.Tests"
let distDir = __SOURCE_DIRECTORY__  @@ "dist"
let distGlob = distDir @@ "*.nupkg"
let toolsDir = __SOURCE_DIRECTORY__  @@ "tools"

let coverageReportDir =  __SOURCE_DIRECTORY__  @@ "docs" @@ "coverage"

let isRelease (targets : Target list) =
    targets
    |> Seq.map(fun t -> t.Name)
    |> Seq.exists ((=)"Release")

let configuration (targets : Target list) =
    let defaultVal = if isRelease targets then "Release" else "Debug"
    match Environment.environVarOrDefault "CONFIGURATION" defaultVal with
     | "Debug" -> DotNet.BuildConfiguration.Debug
     | "Release" -> DotNet.BuildConfiguration.Release
     | config -> DotNet.BuildConfiguration.Custom config

let failOnBadExitAndPrint (p : ProcessResult) =
    if p.ExitCode <> 0 then
        p.Errors |> Seq.iter Trace.traceError
        failwithf "failed with exitcode %d" p.ExitCode

let rec retryIfInCI times fn =
    match Environment.environVarOrNone "CI" with
    | Some _ ->
        if times > 1 then
            try
                fn()
            with
            | _ -> retryIfInCI (times - 1) fn
        else
            fn()
    | _ -> fn()

module dotnet =
    let watch cmdParam program args =
        DotNet.exec cmdParam (sprintf "watch %s" program) args

    let tool optionConfig command args =
        DotNet.exec (fun p -> { p with WorkingDirectory = toolsDir} |> optionConfig ) (sprintf "%s" command) args
        |> failOnBadExitAndPrint

    let fantomas optionConfig args =
        tool optionConfig "fantomas" args

    let reportgenerator optionConfig args =
        tool optionConfig "reportgenerator" args

    let sourcelink optionConfig args =
        tool optionConfig "sourcelink" args



Target.create "Clean" <| fun _ ->
    ["bin"; "temp" ; distDir; coverageReportDir]
    |> Shell.cleanDirs

    !! srcGlob
    ++ testsGlob
    |> Seq.collect(fun p ->
        ["bin";"obj"]
        |> Seq.map(fun sp ->
             IO.Path.GetDirectoryName p @@ sp)
        )
    |> Shell.cleanDirs

Target.create "DotnetRestore" <| fun _ ->
    let exitCode = Shell.Exec("dotnet", "restore", src)
    if exitCode <> 0 then failwith "Could not restore the project"

Target.create "DotnetBuild" <| fun ctx ->
    let exitCode = Shell.Exec("dotnet", "build --configuration Release", src)
    if exitCode <> 0 then failwith "Could not restore the project"

let invokeAsync f = async { f () }

Target.create "DotnetTest" <| fun ctx ->
    let exitCode = Shell.Exec("dotnet", "run", tests)
    if exitCode <> 0 then failwith "Some tests failed"

let mutable dotnetCli = "dotnet"

let publish projectPath =
    [ projectPath </> "bin"
      projectPath </> "obj" ] |> Shell.cleanDirs
    run dotnetCli "restore --no-cache" projectPath
    run dotnetCli "pack -c Release" projectPath
    let nugetKey =
        match Environment.environVarOrNone "NUGET_KEY" with
        | Some nugetKey -> nugetKey
        | None -> failwith "The Nuget API key must be set in a NUGET_KEY environmental variable"
    let nupkg =
        IO.Directory.GetFiles(projectPath </> "bin" </> "Release")
        |> Seq.head
        |> IO.Path.GetFullPath

    let pushCmd = sprintf "nuget push %s -s nuget.org -k %s" nupkg nugetKey
    run dotnetCli pushCmd projectPath

Target.create "PublishNuget" (fun _ -> publish src)

// Only call Clean if DotnetPack was in the call chain
// Ensure Clean is called before DotnetRestore
"Clean" ?=> "DotnetRestore"


"DotnetRestore"
  ==> "DotnetBuild"
  ==> "DotnetTest"
  ==> "PublishNuget"

Target.runOrDefaultWithArguments "DotnetBuild"
