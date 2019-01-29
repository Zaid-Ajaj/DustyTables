module Tests


open Expecto
open DustyTables

[<Tests>]
let tests =
  testList "samples" [
    testCase "Say nothing" <| fun _ ->
      let expected = 1
      let actual = 1
      Expect.equal actual expected "Yeah they should be the same"

  ]
