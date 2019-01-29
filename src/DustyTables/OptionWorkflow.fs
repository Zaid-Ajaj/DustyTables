namespace DustyTables.OptionWorkflow

/// A simple option workflow implementation
type OptionBuilder() =
    member x.Bind(value, map) = Option.bind map value
    member x.Return value = Some value
    member x.ReturnFrom value = value
    member x.Zero () = None

[<AutoOpen>]
module OptionBuilderImplementation = 
    let option = OptionBuilder()