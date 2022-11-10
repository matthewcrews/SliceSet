open System
open Argu
open BenchmarkDotNet.Diagnosers
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Running
open SliceSet
open SliceSet.Domain


type Size =
    | ``100`` = 0
    | ``200`` = 1
    | ``400`` = 2
    | ``800`` = 3
    

[<MemoryDiagnoser>]
type Benchmarks () =

    let rng = Random 123
    let sparsity = 0.01
    let searchPerDimension = 10
    let productCounts =
        [|
            100
            200
            400
            800
        |]
    let supplierCount = 100
    let customerCount = 100
    let dataSets =
        [| for productCount in productCounts do
                seq {
                    for p in 1 .. productCount do
                        for s in 1 .. supplierCount do
                            for c in 1 .. customerCount do
                                let product = Product.create p
                                let supplier = Supplier.create s
                                let customer = Customer.create c
                                product, supplier, customer
                }
                |> Seq.choose (fun entry ->
                    if rng.NextDouble() < sparsity then
                        Some entry
                    else
                        None
                    )
                |> Array.ofSeq
        |]
        
    let productSupplierSearches =
        [| for dataSet in dataSets ->
            [| for _ in 1 .. searchPerDimension ->
                let (product, supplier, _) = dataSet[rng.Next dataSet.Length]
                product, supplier
            |]
        |]
        
    let productCustomerSearches =
        [| for dataSet in dataSets ->
            [| for _ in 1 .. searchPerDimension ->
                let (product, _, customer) = dataSet[rng.Next dataSet.Length]
                product, customer
            |]
        |]
        
    let supplierCustomerSearches =
        [| for dataSet in dataSets ->
            [| for _ in 1 .. searchPerDimension ->
                let (_, supplier, customer) = dataSet[rng.Next dataSet.Length]
                supplier, customer
            |]
        |]
        
        
    let denseNaiveSliceSets =
        dataSets
        |> Array.map Version00.SliceSet3D
    
    let version01DataSets =
        dataSets
        |> Array.map Version01.SliceSet3D

    
    [<Params(Size.``100``, Size.``200``, Size.``400``, Size.``800``)>]
    member val Size = Size.``100`` with get, set
      
    [<Benchmark>]
    member b.DenseNaive () =
        let sliceSet = denseNaiveSliceSets[int b.Size]
        
        let mutable acc = 0
        
        let productSupplierSearch = productSupplierSearches[int b.Size] 
        
        for product, supplier in productSupplierSearch do
            for customer in sliceSet[product, supplier, All] do
                acc <- acc + (int customer)

        acc

    [<Benchmark>]
    member b.RangeIteration () =
        let sliceSet = version01DataSets[int b.Size]
        
        let mutable acc = 0
        
        let productSupplierSearch = productSupplierSearches[int b.Size] 
        
        for product, supplier in productSupplierSearch do
            for customer in sliceSet[product, supplier, All] do
                acc <- acc + (int customer)

        acc

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