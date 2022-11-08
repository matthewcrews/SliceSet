open System
open Argu
open BenchmarkDotNet.Diagnosers
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Running
open SliceSet


[<MemoryDiagnoser>]
type Benchmarks () =

    let rng = Random 123
    let entryCount = 10_000
    let dim1SearchCount = 100
    let dim2SearchCount = 100
    let dim1Min = 0
    let dim1Max = 1000
    let dim2Min = 0
    let dim2Max = 100
    
    let testData = [|
        for _ in 1 .. entryCount ->
            (rng.Next (dim1Min, dim1Max)), (rng.Next (dim2Min, dim2Max))
    |]
        
    let sliceSet = SliceSet2D testData
        
    let dim1Searches = [|
        for _ in 1 .. dim1SearchCount ->
            rng.Next (dim1Min, dim1Max)
    |]
    
    let dim2Searches = [|
        for _ in 1 .. dim2SearchCount ->
            rng.Next (dim2Min, dim2Max)
    |]
      
    [<Benchmark>]
    member _.NaiveFilter () =
        
        let mutable acc = 0
        
        for dim1Search in dim1Searches do
            testData
            |> Array.iter (fun (dim1Value, dim2Value) ->
                if dim1Value = dim1Search then
                    acc <- acc + dim2Value
                )
            
        for dim2Search in dim2Searches do
            testData
            |> Array.iter (fun (dim1Value, dim2Value) ->
                if dim2Value = dim2Search then
                    acc <- acc + dim1Value
                )
            
    [<Benchmark>]
    member _.SliceSet2D () =
        let mutable acc = 0
        
        for dim1Search in dim1Searches do
            for dim2Value in sliceSet[dim1Search, All] do
                acc <- acc + dim2Value

        for dim2Search in dim2Searches do
            for dim1Value in sliceSet[All, dim2Search] do
                acc <- acc + dim1Value


[<RequireQualifiedAccess>]
type Args =
    | Task of task: string
    | Method of method: string
    | Iterations of iterations: int
    
    interface IArgParserTemplate with
        member this.Usage =
            match this with
            | Task _ -> "Which task to perform. Options: Benchmark or Profile"
            | Method _ -> "Which Method to profile. Options: V<number>. <number> = 01 - 10"
            | Iterations _ -> "Number of iterations of the Method to perform for profiling"


let profile (version: string) loopCount =
    ()
    
    
    
[<EntryPoint>]
let main argv =

    printfn $"Args: {argv}"
    
    let parser = ArgumentParser.Create<Args> (programName = "SliceSet.Benchmark")
    let results = parser.Parse argv
    let task = results.GetResult Args.Task

    match task.ToLower() with
    | "benchmark" -> 
        let _ = BenchmarkRunner.Run<Benchmarks>()
        ()

    | "profile" ->
        let method = results.GetResult Args.Method
        let iterations = results.GetResult Args.Iterations
        let _ = profile method iterations
        ()
        
    | unknownTask -> failwith $"Unknown task: {unknownTask}"
    
    1