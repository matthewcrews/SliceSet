module SliceSet.Tests

open System
open NUnit.Framework
open SliceSet.Domain

type Size =
    | ``100`` = 0
    | ``200`` = 1
    | ``400`` = 2
    | ``800`` = 3


let sizes =
    [|
        Size.``100``
        Size.``200``
        Size.``400``
        Size.``800``
    |]

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
    
let productSupplierSearchSets =
    [| for dataSet in dataSets ->
        [| for _ in 1 .. searchPerDimension ->
            let (product, supplier, _) = dataSet[rng.Next dataSet.Length]
            product, supplier
        |]
    |]

let productCustomerSearchSets =
    [| for dataSet in dataSets ->
        [| for _ in 1 .. searchPerDimension ->
            let (product, _, customer) = dataSet[rng.Next dataSet.Length]
            product, customer
        |]
    |]
    
let supplierCustomerSearchSets =
    [| for dataSet in dataSets ->
        [| for _ in 1 .. searchPerDimension ->
            let (_, supplier, customer) = dataSet[rng.Next dataSet.Length]
            supplier, customer
        |]
    |]
    
let baselineSliceSets =
        dataSets
        |> Array.map NaiveFiltering.SliceSet3D
    
let rangeIterationSliceSets =
    dataSets
    |> Array.map LazyIteration.SliceSet3D

let computedRangesSliceSets =
    dataSets
    |> Array.map EagerRangeEval.SliceSet3D

let customerSeriesSliceSets =
    dataSets
    |> Array.map CustomSeries.SliceSet3D

let arrayPoolSliceSets =
    dataSets
    |> Array.map ArrayPool.SliceSet3D
    
let branchlessSliceSets =
    dataSets
    |> Array.map Branchless.SliceSet3D
    
let customPoolSliceSets =
    dataSets
    |> Array.map CustomPool.SliceSet3D
    
let customPool2SliceSets =
    dataSets
    |> Array.map CustomPool2.SliceSet3D
    
let groupIntersectSliceSets =
    dataSets
    |> Array.map GroupIntersect.SliceSet3D
    
let altLoopSliceSets =
    dataSets
    |> Array.map GroupIntersect.SliceSet3D
   
let binarySearchSliceSets =
    dataSets
    |> Array.map BinarySearch.SliceSet3D
   
let skipIndexSliceSets =
    dataSets
    |> Array.map SkipIndex.SliceSet3D
    
let altIndexSliceSets =
    dataSets
    |> Array.map AltIndex.SliceSet3D
  
let branchless2SliceSets =
    dataSets
    |> Array.map Branchless2.SliceSet3D
    
let soaSliceSets =
    dataSets
    |> Array.map SOA.SliceSet3D
    
let doubleBufferSliceSets =
    dataSets
    |> Array.map DoubleBuffer.SliceSet3D
    
let avxSliceSets =
    dataSets
    |> Array.map Avx.SliceSet3D
    
[<Test>]
let ``ParallelRanges results match Naive`` () =
    
    for size in sizes do
        let sizeIdx = int size
        
        let baselineSliceSet = baselineSliceSets[sizeIdx]
        let testSliceSet = rangeIterationSliceSets[sizeIdx]
        
        let productSupplierSearches = productSupplierSearchSets[sizeIdx]
        
        for product, supplier in productSupplierSearches do
            let expected = Set (baselineSliceSet[product, supplier, All])
            let actual = Set (testSliceSet[product, supplier, All].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let productCustomerSearches = productCustomerSearchSets[sizeIdx]
        
        for product, customer in productCustomerSearches do
            let expected = Set (baselineSliceSet[product, All, customer])
            let actual = Set (testSliceSet[product, All, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx]
        
        for supplier, customer in supplierCustomerSearches do
            let expected = Set (baselineSliceSet[All, supplier, customer])
            let actual = Set (testSliceSet[All, supplier, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
            
[<Test>]
let ``ComputedRanges results match Naive`` () =
    
    for size in sizes do
        let sizeIdx = int size
        
        let baselineSliceSet = baselineSliceSets[sizeIdx]
        let testSliceSet = computedRangesSliceSets[sizeIdx]
        
        let productSupplierSearches = productSupplierSearchSets[sizeIdx]
        
        for product, supplier in productSupplierSearches do
            let expected = Set (baselineSliceSet[product, supplier, All])
            let actual = Set (testSliceSet[product, supplier, All].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let productCustomerSearches = productCustomerSearchSets[sizeIdx]
        
        for product, customer in productCustomerSearches do
            let expected = Set (baselineSliceSet[product, All, customer])
            let actual = Set (testSliceSet[product, All, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx]
        
        for supplier, customer in supplierCustomerSearches do
            let expected = Set (baselineSliceSet[All, supplier, customer])
            let actual = Set (testSliceSet[All, supplier, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
            
[<Test>]
let ``CustomSeries results match Naive`` () =
    
    for size in sizes do
        let sizeIdx = int size
        
        let baselineSliceSet = baselineSliceSets[sizeIdx]
        let testSliceSets = customerSeriesSliceSets[sizeIdx]
        
        let productSupplierSearches = productSupplierSearchSets[sizeIdx]
        
        for product, supplier in productSupplierSearches do
            let expected = Set (baselineSliceSet[product, supplier, All])
            let actual = Set (testSliceSets[product, supplier, All].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let productCustomerSearches = productCustomerSearchSets[sizeIdx]
        
        for product, customer in productCustomerSearches do
            let expected = Set (baselineSliceSet[product, All, customer])
            let actual = Set (testSliceSets[product, All, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx]
        
        for supplier, customer in supplierCustomerSearches do
            let expected = Set (baselineSliceSet[All, supplier, customer])
            let actual = Set (testSliceSets[All, supplier, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
            
[<Test>]
let ``ArrayPool results match Naive`` () =
    
    for size in sizes do
        let sizeIdx = int size
        
        let baselineSliceSet = baselineSliceSets[sizeIdx]
        let testSliceSets = arrayPoolSliceSets[sizeIdx]
        
        let productSupplierSearches = productSupplierSearchSets[sizeIdx]
        
        for product, supplier in productSupplierSearches do
            let expected = Set (baselineSliceSet[product, supplier, All])
            let actual = Set (testSliceSets[product, supplier, All].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let productCustomerSearches = productCustomerSearchSets[sizeIdx]
        
        for product, customer in productCustomerSearches do
            let expected = Set (baselineSliceSet[product, All, customer])
            let actual = Set (testSliceSets[product, All, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx]
        
        for supplier, customer in supplierCustomerSearches do
            let expected = Set (baselineSliceSet[All, supplier, customer])
            let actual = Set (testSliceSets[All, supplier, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
            
[<Test>]
let ``Branchless results match Naive`` () =
    
    for size in sizes do
        let sizeIdx = int size
        
        let baselineSliceSet = baselineSliceSets[sizeIdx]
        let testSliceSets = branchlessSliceSets[sizeIdx]
        
        let productSupplierSearches = productSupplierSearchSets[sizeIdx]
        
        for product, supplier in productSupplierSearches do
            let expected = Set (baselineSliceSet[product, supplier, All])
            let actual = Set (testSliceSets[product, supplier, All].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let productCustomerSearches = productCustomerSearchSets[sizeIdx]
        
        for product, customer in productCustomerSearches do
            let expected = Set (baselineSliceSet[product, All, customer])
            let actual = Set (testSliceSets[product, All, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx]
        
        for supplier, customer in supplierCustomerSearches do
            let expected = Set (baselineSliceSet[All, supplier, customer])
            let actual = Set (testSliceSets[All, supplier, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
[<Test>]
let ``CustomPool results match Naive`` () =
    
    for size in sizes do
        let sizeIdx = int size
        
        let baselineSliceSet = baselineSliceSets[sizeIdx]
        let testSliceSets = customPoolSliceSets[sizeIdx]
        
        let productSupplierSearches = productSupplierSearchSets[sizeIdx]
        
        for product, supplier in productSupplierSearches do
            let expected = Set (baselineSliceSet[product, supplier, All])
            let actual = Set (testSliceSets[product, supplier, All].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let productCustomerSearches = productCustomerSearchSets[sizeIdx]
        
        for product, customer in productCustomerSearches do
            let expected = Set (baselineSliceSet[product, All, customer])
            let actual = Set (testSliceSets[product, All, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx]
        
        for supplier, customer in supplierCustomerSearches do
            let expected = Set (baselineSliceSet[All, supplier, customer])
            let actual = Set (testSliceSets[All, supplier, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
[<Test>]
let ``CustomPool2 results match Naive`` () =
    
    for size in sizes do
        let sizeIdx = int size
        
        let baselineSliceSet = baselineSliceSets[sizeIdx]
        let testSliceSets = customPool2SliceSets[sizeIdx]
        
        let productSupplierSearches = productSupplierSearchSets[sizeIdx]
        
        for product, supplier in productSupplierSearches do
            let expected = Set (baselineSliceSet[product, supplier, All])
            let actual = Set (testSliceSets[product, supplier, All].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let productCustomerSearches = productCustomerSearchSets[sizeIdx]
        
        for product, customer in productCustomerSearches do
            let expected = Set (baselineSliceSet[product, All, customer])
            let actual = Set (testSliceSets[product, All, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx]
        
        for supplier, customer in supplierCustomerSearches do
            let expected = Set (baselineSliceSet[All, supplier, customer])
            let actual = Set (testSliceSets[All, supplier, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
    
[<Test>]
let ``GroupIntersect results match Naive`` () =
    
    for size in sizes do
        let sizeIdx = int size
        
        let baselineSliceSet = baselineSliceSets[sizeIdx]
        let testSliceSets = groupIntersectSliceSets[sizeIdx]
        
        let productSupplierSearches = productSupplierSearchSets[sizeIdx]
        
        for product, supplier in productSupplierSearches do
            let expected = Set (baselineSliceSet[product, supplier, All])
            let actual = Set (testSliceSets[product, supplier, All].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let productCustomerSearches = productCustomerSearchSets[sizeIdx]
        
        for product, customer in productCustomerSearches do
            let expected = Set (baselineSliceSet[product, All, customer])
            let actual = Set (testSliceSets[product, All, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx]
        
        for supplier, customer in supplierCustomerSearches do
            let expected = Set (baselineSliceSet[All, supplier, customer])
            let actual = Set (testSliceSets[All, supplier, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            

[<Test>]
let ``AltLoop results match Naive`` () =
    
    for size in sizes do
        let sizeIdx = int size
        
        let baselineSliceSet = baselineSliceSets[sizeIdx]
        let testSliceSets = altLoopSliceSets[sizeIdx]
        
        let productSupplierSearches = productSupplierSearchSets[sizeIdx]
        
        for product, supplier in productSupplierSearches do
            let expected = Set (baselineSliceSet[product, supplier, All])
            let actual = Set (testSliceSets[product, supplier, All].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let productCustomerSearches = productCustomerSearchSets[sizeIdx]
        
        for product, customer in productCustomerSearches do
            let expected = Set (baselineSliceSet[product, All, customer])
            let actual = Set (testSliceSets[product, All, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx]
        
        for supplier, customer in supplierCustomerSearches do
            let expected = Set (baselineSliceSet[All, supplier, customer])
            let actual = Set (testSliceSets[All, supplier, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
            
[<Test>]
let ``BinarySearch results match Naive`` () =
    
    for size in sizes do
        let sizeIdx = int size
        
        let baselineSliceSet = baselineSliceSets[sizeIdx]
        let testSliceSets = binarySearchSliceSets[sizeIdx]
        
        let productSupplierSearches = productSupplierSearchSets[sizeIdx]
        
        for product, supplier in productSupplierSearches do
            let expected = Set (baselineSliceSet[product, supplier, All])
            let actual = Set (testSliceSets[product, supplier, All].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let productCustomerSearches = productCustomerSearchSets[sizeIdx]
        
        for product, customer in productCustomerSearches do
            let expected = Set (baselineSliceSet[product, All, customer])
            let actual = Set (testSliceSets[product, All, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx]
        
        for supplier, customer in supplierCustomerSearches do
            let expected = Set (baselineSliceSet[All, supplier, customer])
            let actual = Set (testSliceSets[All, supplier, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
    
[<Test>]
let ``SkipIndex results match Naive`` () =
    
    for size in sizes do
        let sizeIdx = int size
        
        let baselineSliceSet = baselineSliceSets[sizeIdx]
        let testSliceSets = skipIndexSliceSets[sizeIdx]
        
        let productSupplierSearches = productSupplierSearchSets[sizeIdx]
        
        for product, supplier in productSupplierSearches do
            let expected = Set (baselineSliceSet[product, supplier, All])
            let actual = Set (testSliceSets[product, supplier, All].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let productCustomerSearches = productCustomerSearchSets[sizeIdx]
        
        for product, customer in productCustomerSearches do
            let expected = Set (baselineSliceSet[product, All, customer])
            let actual = Set (testSliceSets[product, All, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx]
        
        for supplier, customer in supplierCustomerSearches do
            let expected = Set (baselineSliceSet[All, supplier, customer])
            let subset = testSliceSets[All, supplier, customer]
            let actual = Set (subset.AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
    
[<Test>]
let ``AltIndex results match Naive`` () =
    
    for size in sizes do
        let sizeIdx = int size
        
        let baselineSliceSet = baselineSliceSets[sizeIdx]
        let testSliceSets = altIndexSliceSets[sizeIdx]
        
        let productSupplierSearches = productSupplierSearchSets[sizeIdx]
        
        for product, supplier in productSupplierSearches do
            let expected = Set (baselineSliceSet[product, supplier, All])
            let actual = Set (testSliceSets[product, supplier, All].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let productCustomerSearches = productCustomerSearchSets[sizeIdx]
        
        for product, customer in productCustomerSearches do
            let expected = Set (baselineSliceSet[product, All, customer])
            let actual = Set (testSliceSets[product, All, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx]
        
        for supplier, customer in supplierCustomerSearches do
            let expected = Set (baselineSliceSet[All, supplier, customer])
            let subset = testSliceSets[All, supplier, customer]
            let actual = Set (subset.AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
            
[<Test>]
let ``Branchless2 results match Naive`` () =
    
    for size in sizes do
        let sizeIdx = int size
        
        let baselineSliceSet = baselineSliceSets[sizeIdx]
        let testSliceSets = branchless2SliceSets[sizeIdx]
        
        let productSupplierSearches = productSupplierSearchSets[sizeIdx]
        
        for product, supplier in productSupplierSearches do
            let expected = Set (baselineSliceSet[product, supplier, All])
            let actual = Set (testSliceSets[product, supplier, All].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let productCustomerSearches = productCustomerSearchSets[sizeIdx]
        
        for product, customer in productCustomerSearches do
            let expected = Set (baselineSliceSet[product, All, customer])
            let actual = Set (testSliceSets[product, All, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx]
        
        for supplier, customer in supplierCustomerSearches do
            let expected = Set (baselineSliceSet[All, supplier, customer])
            let subset = testSliceSets[All, supplier, customer]
            let actual = Set (subset.AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
            
[<Test>]
let ``SOA results match Naive`` () =
    
    for size in sizes do
        let sizeIdx = int size
        
        let baselineSliceSet = baselineSliceSets[sizeIdx]
        let testSliceSets = soaSliceSets[sizeIdx]
        
        let productSupplierSearches = productSupplierSearchSets[sizeIdx]
        
        for product, supplier in productSupplierSearches do
            let expected = Set (baselineSliceSet[product, supplier, All])
            let actual = Set (testSliceSets[product, supplier, All].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let productCustomerSearches = productCustomerSearchSets[sizeIdx]
        
        for product, customer in productCustomerSearches do
            let expected = Set (baselineSliceSet[product, All, customer])
            let actual = Set (testSliceSets[product, All, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx]
        
        for supplier, customer in supplierCustomerSearches do
            let expected = Set (baselineSliceSet[All, supplier, customer])
            let subset = testSliceSets[All, supplier, customer]
            let actual = Set (subset.AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
            
[<Test>]
let ``Builder results match Naive`` () =
    
    for size in sizes do
        let sizeIdx = int size
        
        let baselineSliceSet = baselineSliceSets[sizeIdx]
        let testSliceSets = doubleBufferSliceSets[sizeIdx]
        
        let productSupplierSearches = productSupplierSearchSets[sizeIdx]
        
        for product, supplier in productSupplierSearches do
            let expected = Set (baselineSliceSet[product, supplier, All])
            let actual = Set (testSliceSets[product, supplier, All].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let productCustomerSearches = productCustomerSearchSets[sizeIdx]
        
        for product, customer in productCustomerSearches do
            let expected = Set (baselineSliceSet[product, All, customer])
            let actual = Set (testSliceSets[product, All, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx]
        
        for supplier, customer in supplierCustomerSearches do
            let expected = Set (baselineSliceSet[All, supplier, customer])
            let subset = testSliceSets[All, supplier, customer]
            let actual = Set (subset.AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
[<Test>]
let ``Avx results match Naive`` () =
    
    for size in sizes do
        let sizeIdx = int size
        
        let baselineSliceSet = baselineSliceSets[sizeIdx]
        let testSliceSets = avxSliceSets[sizeIdx]
        
        let productSupplierSearches = productSupplierSearchSets[sizeIdx]
        
        for product, supplier in productSupplierSearches do
            let expected = Set (baselineSliceSet[product, supplier, All])
            let actual = Set (testSliceSets[product, supplier, All].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let productCustomerSearches = productCustomerSearchSets[sizeIdx]
        
        for product, customer in productCustomerSearches do
            let expected = Set (baselineSliceSet[product, All, customer])
            let actual = Set (testSliceSets[product, All, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx]
        
        for supplier, customer in supplierCustomerSearches do
            let expected = Set (baselineSliceSet[All, supplier, customer])
            let subset = testSliceSets[All, supplier, customer]
            let actual = Set (subset.AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
