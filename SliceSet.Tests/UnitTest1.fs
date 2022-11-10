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
        |> Array.map Version00.SliceSet3D
    
let rangeIterationSliceSets =
    dataSets
    |> Array.map Version01.SliceSet3D


[<Test>]
let ``RangeIteration results match Naive`` () =
    
    for size in sizes do
        let sizeIdx = int size
        
        let baselineSliceSet = baselineSliceSets[sizeIdx]
        let rangeIterationSliceSet = rangeIterationSliceSets[sizeIdx]
        
        let productSupplierSearches = productSupplierSearchSets[sizeIdx]
        
        for product, supplier in productSupplierSearches do
            let expected = Set (baselineSliceSet[product, supplier, All])
            let actual = Set (rangeIterationSliceSet[product, supplier, All].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let productCustomerSearches = productCustomerSearchSets[sizeIdx]
        
        for product, customer in productCustomerSearches do
            let expected = Set (baselineSliceSet[product, All, customer])
            let actual = Set (rangeIterationSliceSet[product, All, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)
            
        let supplierCustomerSearches = supplierCustomerSearchSets[sizeIdx]
        
        for supplier, customer in supplierCustomerSearches do
            let expected = Set (baselineSliceSet[All, supplier, customer])
            let actual = Set (rangeIterationSliceSet[All, supplier, customer].AsSeq())
            
            // Make sure we actually got a result
            Assert.Greater (expected.Count, 0)
            // Test that the values are equivalent
            Assert.AreEqual (expected, actual)