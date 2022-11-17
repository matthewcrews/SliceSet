open System
open System.Collections.Generic
open Argu
open BenchmarkDotNet.Diagnosers
open BenchmarkDotNet.Attributes
open BenchmarkDotNet.Running
open SliceSet
open SliceSet.Domain


type ProductCount =
    | ``100`` = 0
    | ``200`` = 1
    | ``400`` = 2
    | ``800`` = 3
    | ``1600`` = 4
    
type Sparsity =
    | ``0.1%`` = 0
    | ``1.0%`` = 1
    | ``10%`` = 2
    

[<MemoryDiagnoser>]
[<HardwareCounters(
    HardwareCounter.BranchMispredictions,
    HardwareCounter.BranchInstructions,
    HardwareCounter.CacheMisses
    // HardwareCounter.TotalCycles,
    // HardwareCounter.TotalIssues,
    // HardwareCounter.InstructionRetired
    )>]
[<DisassemblyDiagnoser(printSource=true, exportCombinedDisassemblyReport=true, exportHtml=true, exportGithubMarkdown=true, printInstructionAddresses=true, maxDepth=3)>]
type Benchmarks () =

    let rng = Random 123
    let searchPerDimension = 10
    let productCounts =
        [|
            ProductCount.``100``
            ProductCount.``200``
            ProductCount.``400``
            ProductCount.``800``
            ProductCount.``1600``
        |]
        
    let productCountValues =
        [|
            100
            200
            400
            800
            1600
        |]

    let sparsities =
        [|
            0.1 / 100.0
            1.0 / 100.0
            10.0 / 100.0
        |]
    
    let supplierCount = 100
    let customerCount = 100
    
    let dataSets =
        [| for productCount in productCounts do
            let productCountValue = productCountValues[int productCount]
            [| for sparsity in sparsities do
                seq {
                    for p in 1 .. productCountValue do
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
        |]
    
    let productSupplierSearchSets =
        [| for productCount in productCounts do
            [| for sparsity in sparsities do
                let data = dataSets[int productCount][int sparsity]
                [| for _ in 1 .. searchPerDimension ->
                    let (product, supplier, _) = data[rng.Next data.Length]
                    product, supplier
                |]
            |]
        |]
        
    let productCustomerSearchSets =
        [| for productCount in productCounts do
            [| for sparsity in sparsities do
                let data = dataSets[int productCount][int sparsity]
                [| for _ in 1 .. searchPerDimension ->
                    let (product, _, customer) = data[rng.Next data.Length]
                    product, customer
                |]
            |]
        |]
        
    let supplierCustomerSearchSets =
        [| for productCount in productCounts do
            [| for sparsity in sparsities do
                let data = dataSets[int productCount][int sparsity]
                [| for _ in 1 .. searchPerDimension ->
                    let (_, supplier, customer) = data[rng.Next data.Length]
                    supplier, customer
                |]
            |]
        |]
        
        
    let denseNaiveSliceSets =
        dataSets
        |> Array.map (Array.map NaiveFiltering.SliceSet3D)
    
    let rangeIterationSliceSets =
        dataSets
        |> Array.map (Array.map LazyIteration.SliceSet3D)

    let computedRangeSliceSets =
        dataSets
        |> Array.map (Array.map EagerRangeEval.SliceSet3D)
    
    let customSeriesSliceSets =
        dataSets
        |> Array.map (Array.map CustomSeries.SliceSet3D)
    
    let arrayPoolSliceSets =
        dataSets
        |> Array.map (Array.map ArrayPool.SliceSet3D)
    
    let branchlessSliceSets =
        dataSets
        |> Array.map (Array.map Branchless.SliceSet3D)
        
    let customPoolSliceSets =
        dataSets
        |> Array.map (Array.map CustomPool.SliceSet3D)
        
    let customPool2SliceSets =
        dataSets
        |> Array.map (Array.map CustomPool2.SliceSet3D)
        
    let groupIntersectSliceSets =
        dataSets
        |> Array.map (Array.map GroupIntersect.SliceSet3D)
    
    let altLoopSliceSets =
        dataSets
        |> Array.map (Array.map AltLoop.SliceSet3D)
    
    let binarySearchSliceSets =
        dataSets
        |> Array.map (Array.map BinarySearch.SliceSet3D)
    
    let skipIndexSliceSets =
        dataSets
        |> Array.map (Array.map SkipIndex.SliceSet3D)
    
    [<Params(ProductCount.``100``, ProductCount.``200``, ProductCount.``400``, ProductCount.``800``)>]
    // [<Params(ProductCount.``800``)>]
    member val Size = ProductCount.``800`` with get, set
    
    // [<Params(Sparsity.``0.1%``, Sparsity.``1.0%``, Sparsity.``10%``)>]
    [<Params(Sparsity.``0.1%``)>]
    member val Sparsity = Sparsity.``0.1%`` with get, set
      
    // [<Benchmark>]
    member b.NaiveFilter () =
        let sizeIdx = int b.Size
        let sparsityIdx = int b.Sparsity
        let sliceSet = denseNaiveSliceSets[sizeIdx][sparsityIdx]
        
        let mutable acc = 0
        
        let productSupplierSearch = productSupplierSearchSets[sizeIdx][sparsityIdx]
        
        for product, supplier in productSupplierSearch do
            for customer in sliceSet[product, supplier, All] do
                acc <- acc + (int customer)
                
        let productCustomerSearches = productCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for product, customer in productCustomerSearches do
            for supplier in sliceSet[product, All, customer] do
                acc <- acc + (int supplier)
                
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for supplier, customer in supplierCustomerSearches do
            for product in sliceSet[All, supplier, customer] do
                acc <- acc + (int product)
    
        acc

    
    // [<Benchmark>]
    member b.ParallelRanges () =
        let sizeIdx = int b.Size
        let sparsityIdx = int b.Sparsity
        let sliceSet = rangeIterationSliceSets[sizeIdx][sparsityIdx]
        
        let mutable acc = 0
        
        let productSupplierSearch = productSupplierSearchSets[sizeIdx][sparsityIdx]
        
        for product, supplier in productSupplierSearch do
            for customer in sliceSet[product, supplier, All] do
                acc <- acc + (int customer)
                
        let productCustomerSearches = productCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for product, customer in productCustomerSearches do
            for supplier in sliceSet[product, All, customer] do
                acc <- acc + (int supplier)
                
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for supplier, customer in supplierCustomerSearches do
            for product in sliceSet[All, supplier, customer] do
                acc <- acc + (int product)

        acc
        
    
    // [<Benchmark>]
    member b.ComputedRanges () =
        let sizeIdx = int b.Size
        let sparsityIdx = int b.Sparsity
        let sliceSet = computedRangeSliceSets[sizeIdx][sparsityIdx]
        
        let mutable acc = 0
        
        let productSupplierSearch = productSupplierSearchSets[sizeIdx][sparsityIdx]
        
        for product, supplier in productSupplierSearch do
            for customer in sliceSet[product, supplier, All] do
                acc <- acc + (int customer)
                
        let productCustomerSearches = productCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for product, customer in productCustomerSearches do
            for supplier in sliceSet[product, All, customer] do
                acc <- acc + (int supplier)
                
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for supplier, customer in supplierCustomerSearches do
            for product in sliceSet[All, supplier, customer] do
                acc <- acc + (int product)

        acc
        
        
    // [<Benchmark>]
    member b.CustomSeries () =
        let sizeIdx = int b.Size
        let sparsityIdx = int b.Sparsity
        let sliceSet = customSeriesSliceSets[sizeIdx][sparsityIdx]
        
        let mutable acc = 0
        
        let productSupplierSearch = productSupplierSearchSets[sizeIdx][sparsityIdx]
        
        for product, supplier in productSupplierSearch do
            for customer in sliceSet[product, supplier, All] do
                acc <- acc + (int customer)
                
        let productCustomerSearches = productCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for product, customer in productCustomerSearches do
            for supplier in sliceSet[product, All, customer] do
                acc <- acc + (int supplier)
                
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for supplier, customer in supplierCustomerSearches do
            for product in sliceSet[All, supplier, customer] do
                acc <- acc + (int product)

        acc

    [<Benchmark>]
    member b.ArrayPool () =
        let sizeIdx = int b.Size
        let sparsityIdx = int b.Sparsity
        let sliceSet = arrayPoolSliceSets[sizeIdx][sparsityIdx]
        
        let mutable acc = 0
        
        let productSupplierSearch = productSupplierSearchSets[sizeIdx][sparsityIdx]
        
        for product, supplier in productSupplierSearch do
            for customer in sliceSet[product, supplier, All] do
                acc <- acc + (int customer)
                
        let productCustomerSearches = productCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for product, customer in productCustomerSearches do
            for supplier in sliceSet[product, All, customer] do
                acc <- acc + (int supplier)
                
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for supplier, customer in supplierCustomerSearches do
            for product in sliceSet[All, supplier, customer] do
                acc <- acc + (int product)

        acc        
        
    // [<Benchmark>]
    member b.Branchless () =
        let sizeIdx = int b.Size
        let sparsityIdx = int b.Sparsity
        let sliceSet = branchlessSliceSets[sizeIdx][sparsityIdx]
        
        let mutable acc = 0
        
        let productSupplierSearch = productSupplierSearchSets[sizeIdx][sparsityIdx]
        
        for product, supplier in productSupplierSearch do
            for customer in sliceSet[product, supplier, All] do
                acc <- acc + (int customer)
                
        let productCustomerSearches = productCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for product, customer in productCustomerSearches do
            for supplier in sliceSet[product, All, customer] do
                acc <- acc + (int supplier)
                
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for supplier, customer in supplierCustomerSearches do
            for product in sliceSet[All, supplier, customer] do
                acc <- acc + (int product)

        acc
        
    // [<Benchmark>]
    member b.CustomPool () =
        let sizeIdx = int b.Size
        let sparsityIdx = int b.Sparsity
        let sliceSet = customPoolSliceSets[sizeIdx][sparsityIdx]
        
        let mutable acc = 0
        
        let productSupplierSearch = productSupplierSearchSets[sizeIdx][sparsityIdx]
        
        for product, supplier in productSupplierSearch do
            for customer in sliceSet[product, supplier, All] do
                acc <- acc + (int customer)
                
        let productCustomerSearches = productCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for product, customer in productCustomerSearches do
            for supplier in sliceSet[product, All, customer] do
                acc <- acc + (int supplier)
                
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for supplier, customer in supplierCustomerSearches do
            for product in sliceSet[All, supplier, customer] do
                acc <- acc + (int product)

        acc 

    
    // [<Benchmark>]
    member b.CustomPool2 () =
        let sizeIdx = int b.Size
        let sparsityIdx = int b.Sparsity
        let sliceSet = customPool2SliceSets[sizeIdx][sparsityIdx]
        
        let mutable acc = 0
        
        let productSupplierSearch = productSupplierSearchSets[sizeIdx][sparsityIdx]
        
        for product, supplier in productSupplierSearch do
            for customer in sliceSet[product, supplier, All] do
                acc <- acc + (int customer)
                
        let productCustomerSearches = productCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for product, customer in productCustomerSearches do
            for supplier in sliceSet[product, All, customer] do
                acc <- acc + (int supplier)
                
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for supplier, customer in supplierCustomerSearches do
            for product in sliceSet[All, supplier, customer] do
                acc <- acc + (int product)

        acc
        
    // [<Benchmark>]
    member b.GroupIntersect () =
        let sizeIdx = int b.Size
        let sparsityIdx = int b.Sparsity
        let sliceSet = groupIntersectSliceSets[sizeIdx][sparsityIdx]
        
        let mutable acc = 0
        
        let productSupplierSearch = productSupplierSearchSets[sizeIdx][sparsityIdx]
        
        for product, supplier in productSupplierSearch do
            for customer in sliceSet[product, supplier, All] do
                acc <- acc + (int customer)
                
        let productCustomerSearches = productCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for product, customer in productCustomerSearches do
            for supplier in sliceSet[product, All, customer] do
                acc <- acc + (int supplier)
                
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for supplier, customer in supplierCustomerSearches do
            for product in sliceSet[All, supplier, customer] do
                acc <- acc + (int product)

        acc 


    // [<Benchmark>]
    member b.AltLoop () =
        let sizeIdx = int b.Size
        let sparsityIdx = int b.Sparsity
        let sliceSet = altLoopSliceSets[sizeIdx][sparsityIdx]
        
        let mutable acc = 0
        
        let productSupplierSearch = productSupplierSearchSets[sizeIdx][sparsityIdx]
        
        for product, supplier in productSupplierSearch do
            for customer in sliceSet[product, supplier, All] do
                acc <- acc + (int customer)
                
        let productCustomerSearches = productCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for product, customer in productCustomerSearches do
            for supplier in sliceSet[product, All, customer] do
                acc <- acc + (int supplier)
                
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for supplier, customer in supplierCustomerSearches do
            for product in sliceSet[All, supplier, customer] do
                acc <- acc + (int product)

        acc 

    // [<Benchmark>]
    member b.BinarySearch () =
        let sizeIdx = int b.Size
        let sparsityIdx = int b.Sparsity
        let sliceSet = binarySearchSliceSets[sizeIdx][sparsityIdx]
        
        let mutable acc = 0
        
        let productSupplierSearch = productSupplierSearchSets[sizeIdx][sparsityIdx]
        
        for product, supplier in productSupplierSearch do
            for customer in sliceSet[product, supplier, All] do
                acc <- acc + (int customer)
                
        let productCustomerSearches = productCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for product, customer in productCustomerSearches do
            for supplier in sliceSet[product, All, customer] do
                acc <- acc + (int supplier)
                
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for supplier, customer in supplierCustomerSearches do
            for product in sliceSet[All, supplier, customer] do
                acc <- acc + (int product)

        acc 

    [<Benchmark>]
    member b.SkipIndex () =
        let sizeIdx = int b.Size
        let sparsityIdx = int b.Sparsity
        let sliceSet = skipIndexSliceSets[sizeIdx][sparsityIdx]
        
        let mutable acc = 0
        
        let productSupplierSearch = productSupplierSearchSets[sizeIdx][sparsityIdx]
        
        for product, supplier in productSupplierSearch do
            for customer in sliceSet[product, supplier, All] do
                acc <- acc + (int customer)
                
        let productCustomerSearches = productCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for product, customer in productCustomerSearches do
            for supplier in sliceSet[product, All, customer] do
                acc <- acc + (int supplier)
                
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx][sparsityIdx]
        
        for supplier, customer in supplierCustomerSearches do
            for product in sliceSet[All, supplier, customer] do
                acc <- acc + (int product)

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
    printfn $"Profiling: {version}, LoopCount: {loopCount}"
    let b = Benchmarks ()
    let mutable result = 0
    
    printfn "Starting Loops..."
    match version.ToLower() with
    | "naivefilter" ->
        for _ in 1 .. loopCount do
            result <- result + b.NaiveFilter()
            
    | "parallelranges" ->
        for _ in 1 .. loopCount do
            result <- result + b.ParallelRanges()
            
    | "computedranges" ->
        for _ in 1 .. loopCount do
            result <- result + b.ComputedRanges()
            
    | "customseries" ->
        for _ in 1 .. loopCount do
            result <- result + b.CustomSeries()
            
    | "arraypool" ->
        for _ in 1 .. loopCount do
            result <- result + b.ArrayPool()
            
    | "branchless" ->
        for _ in 1 .. loopCount do
            result <- result + b.Branchless()
            
    | "custompool" ->
        for _ in 1 .. loopCount do
            result <- result + b.CustomPool()
            
    | "custompool2" ->
        for _ in 1 .. loopCount do
            result <- result + b.CustomPool2()
            
    | "groupintersect" ->
        for _ in 1 .. loopCount do
            result <- result + b.GroupIntersect()
            
    | "altloop" ->
        for _ in 1 .. loopCount do
            result <- result + b.AltLoop()
            
    | "binarysearch" ->
        for _ in 1 .. loopCount do
            result <- result + b.BinarySearch()
            
    | "skipindex" ->
        for _ in 1 .. loopCount do
            result <- result + b.SkipIndex()
            
    | unknownVersion -> failwith $"Unknown version: {unknownVersion}" 
        
    result
    
    
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