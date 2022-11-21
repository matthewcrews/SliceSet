namespace SliceSet.Avx

open System
open System.Buffers
open System.Collections.Generic
open System.Numerics
open System.Runtime.Intrinsics.X86
open System.Runtime.Intrinsics
open Microsoft.FSharp.NativeInterop
open SliceSet.Collections
open SliceSet.Domain

#nowarn "9"
#nowarn "42"
#nowarn "51"

[<Struct>]
type Range<[<Measure>] 'Measure> =
    {
        Start : int<'Measure>
        // This is the EXCLUSIVE upper bound
        Bound : int<'Measure>
    }
     
type Series<[<Measure>] 'Measure> =
    {
        Starts : int<'Measure>[]
        Bounds : int<'Measure>[]
    }
    member s.Length = s.Starts.Length

module Series =
    
    module private Helpers =
        
        let inline retype<'T,'U> (x: 'T) : 'U = (# "" x: 'U #)
        
        let leftCompactShuffleMasks : Vector128<byte>[] =
            // NOTE: Remember x86 is little-endian therefore we need to select
            // Also note, we are using this with Shuffle, which should be thought
            // of as a Selector of the elements, not masking elements off.
            let zero = 0x80_80_80_80 // Zero
            let elm0 = 0x03_02_01_00 // 0th position
            let elm1 = 0x07_06_05_04 // 1st position
            let elm2 = 0x0B_0A_09_08 // 2nd position
            let elm3 = 0x0F_0E_0D_0C // 3rd position
            
            retype [|
                Vector128.Create (zero, zero, zero, zero) // BitMask Pattern: 0000
                Vector128.Create (elm0, zero, zero, zero) // BitMask Pattern: 0001
                Vector128.Create (elm1, zero, zero, zero) // BitMask Pattern: 0010
                Vector128.Create (elm0, elm1, zero, zero) // BitMask Pattern: 0011
                Vector128.Create (elm2, zero, zero, zero) // BitMask Pattern: 0100
                Vector128.Create (elm0, elm2, zero, zero) // BitMask Pattern: 0101
                Vector128.Create (elm1, elm2, zero, zero) // BitMask Pattern: 0110
                Vector128.Create (elm0, elm1, elm2, zero) // BitMask Pattern: 0111
                Vector128.Create (elm3, zero, zero, zero) // BitMask Pattern: 1000
                Vector128.Create (elm0, elm3, zero, zero) // BitMask Pattern: 1001
                Vector128.Create (elm1, elm3, zero, zero) // BitMask Pattern: 1010
                Vector128.Create (elm0, elm1, elm3, zero) // BitMask Pattern: 1011
                Vector128.Create (elm2, elm3, zero, zero) // BitMask Pattern: 1100
                Vector128.Create (elm0, elm2, elm3, zero) // BitMask Pattern: 1101
                Vector128.Create (elm1, elm2, elm3, zero) // BitMask Pattern: 1110
                Vector128.Create (elm0, elm1, elm2, elm3) // BitMask Pattern: 1111
            |]

        
    let ofRanges (ranges: Range<'Measure>[]) : Series<'Measure> =
        let starts =
            ranges
            |> Array.map (fun r -> r.Start)
            
        let bounds =
            ranges
            |> Array.map (fun r -> r.Bound)
            
        {
            Starts = starts
            Bounds = bounds
        }
    
    let all (length: int) =
        {
            Starts = [| LanguagePrimitives.Int32WithMeasure<'Measure> 0 |]
            Bounds = [| LanguagePrimitives.Int32WithMeasure<'Measure> length |]
        }
    
    let empty<[<Measure>] 'Measure> : Series<'Measure> =
        {
            Starts = Array.empty
            Bounds = Array.empty
        }
    
    let intersect (a: Series<'Measure>) (b: Series<'Measure>) : Series<'Measure> =
        if a.Length = 0 || b.Length = 0 then
            empty
            
        else
            // Have a be the shorter Series
            let a, b = if a.Length < b.Length then a, b else b, a
            
            let accStarts = ArrayPool.Shared.Rent (a.Length + b.Length)
            let accBounds = ArrayPool.Shared.Rent (a.Length + b.Length)
            // AVX does not play nice with Units of Measure
            let aStarts : int[] = Helpers.retype a.Starts
            let bStarts : int[] = Helpers.retype b.Starts
            let aBounds : int[] = Helpers.retype a.Bounds
            let bBounds : int[] = Helpers.retype b.Bounds
            
            let mutable aIdx = 0
            let mutable bIdx = 0
            let mutable accIdx = 0

            // Only want to perform this loop if Avx2 is supported
            if Avx2.IsSupported then
                
                let lastBlockIdx = Vector128<int>.Count * (bStarts.Length / Vector128<int>.Count)
                let bStartsPtr = && bStarts.AsSpan().GetPinnableReference()
                let bBoundsPtr = && bBounds.AsSpan().GetPinnableReference()
                let accStartsPtr = && accStarts.AsSpan().GetPinnableReference()
                let accBoundsPtr = && accBounds.AsSpan().GetPinnableReference()
                
                while aIdx < aStarts.Length && bIdx < lastBlockIdx do

                    if aBounds[aIdx] <= bStarts[bIdx] then
                        aIdx <- aIdx + 1
                    elif bBounds[bIdx + Vector128<int>.Count - 1] <= aStarts[aIdx] then
                        bIdx <- bIdx + Vector128<int>.Count
                    else
                        
                        // Load the data into the SIMD registers
                        let aStartVec = Vector128.Create aStarts[aIdx]
                        let aBoundVec = Vector128.Create aBounds[aIdx]
                        let bStartsVec = Avx2.LoadVector128 (NativePtr.add bStartsPtr bIdx)
                        let bBoundsVec = Avx2.LoadVector128 (NativePtr.add bBoundsPtr bIdx)
                        
                        // Compute new Starts and Bounds
                        let newStarts = Avx2.Max (aStartVec, bStartsVec)
                        let newBounds = Avx2.Min (aBoundVec, bBoundsVec)
                        
                        // Perform comparison to check for valid intervals
                        let nonNegativeCheck = Avx2.CompareGreaterThan (newBounds, newStarts)
                        
                        // Retype so we can use MoveMask
                        let nonNegativeCheckAsFloat32 : Vector128<float32> = Helpers.retype nonNegativeCheck
                        
                        // Compute the MoveMask to lookup Left-Compacting shuffle mask
                        let moveMask = Avx2.MoveMask nonNegativeCheckAsFloat32
                        // Lookup the Left-Compacting shuffle mask we will need
                        let shuffleMask = Helpers.leftCompactShuffleMasks[moveMask]
                        
                        // Retype moveMask to use it with PopCount to get number of matches
                        let moveMask : uint32 = Helpers.retype moveMask
                        let numberOfMatches = BitOperations.PopCount moveMask
                        
                        // Retype newStarts and newBounds for shuffling
                        let newStartsAsBytes : Vector128<byte> = Helpers.retype newStarts
                        let newBoundsAsBytes : Vector128<byte> = Helpers.retype newBounds
                        
                        // Shuffle the values that we want to keep
                        let newStartsPacked : Vector128<int> = Helpers.retype (Avx2.Shuffle (newStartsAsBytes, shuffleMask))
                        let newBoundsPacked : Vector128<int> = Helpers.retype (Avx2.Shuffle (newBoundsAsBytes, shuffleMask))
                        
                        // Write the values out to the acc arrays
                        Avx2.Store (NativePtr.add accStartsPtr accIdx, newStartsPacked)
                        Avx2.Store (NativePtr.add accBoundsPtr accIdx, newBoundsPacked)
                        
                        // Move the accIdx forward so that we write new matches to the correct spot
                        accIdx <- accIdx + numberOfMatches
                        if aBounds[aIdx] < bBounds[bIdx + Vector128<int>.Count - 1] then
                            aIdx <- aIdx + 1
                        else
                            bIdx <- bIdx + Vector128<int>.Count
                
            while aIdx < aStarts.Length && bIdx < bStarts.Length do
                
                if aBounds[aIdx] <= bStarts[bIdx] then
                    aIdx <- aIdx + 1
                elif bBounds[bIdx] <= aStarts[aIdx] then
                    bIdx <- bIdx + 1
                else
                    accStarts[accIdx] <- Math.Max (aStarts[aIdx], bStarts[bIdx])
                    accBounds[accIdx] <- Math.Min (aBounds[aIdx], bBounds[bIdx])
                    if accStarts[accIdx] >= accBounds[accIdx] then
                        failwith "Cannot have Start greater or equal to Bound"
                    accIdx <- accIdx + 1
                    
                    if aBounds[aIdx] < bBounds[bIdx] then
                        aIdx <- aIdx + 1
                    else
                        bIdx <- bIdx + 1
                        
            let resStarts = GC.AllocateUninitializedArray accIdx
            let resBounds = GC.AllocateUninitializedArray accIdx
            
            // Copy out results    
            Array.Copy (accStarts, resStarts, accIdx)
            Array.Copy (accBounds, resBounds, accIdx)
            
            // Return acc arrays
            ArrayPool.Shared.Return accStarts
            ArrayPool.Shared.Return accBounds
            
            {
                Starts = resStarts
                Bounds = resBounds
            }


type ValueIndex<'T> =
    {
        ValueSeries : Dictionary<'T, Series<Units.ValueKey>>
        Values : bar<Units.ValueKey, 'T>
    }

     
module ValueIndex =
    
    let create (values: 'T[]) =
        let ranges = Dictionary<'T, Queue<_>>()
        let mutable value = values[0]
        let mutable start = 0<Units.ValueKey>
        let mutable length = 0<Units.ValueKey>

        for i = 0 to values.Length - 1 do
            let valueKey = i * 1<Units.ValueKey>
            
            if values[i] = value then
                length <- length + 1<_>
            else
                // Create the new range
                let range = { Start = start; Bound = start + length }
                // Get the Range queue for the current value
                if not (ranges.ContainsKey value) then
                    ranges[value] <- Queue()
                    
                ranges[value].Enqueue range

                // Reset the mutable values
                value <- values[i]
                start <- valueKey
                length <- 1<_>

        // Wrap up the last Range the loop was working on
        // Create the new range
        let range = { Start = start; Bound = start + length }
        // Get the Range queue for the current value
        if not (ranges.ContainsKey value) then
            ranges[value] <- Queue()
            
        ranges[value].Enqueue range

        // We now want to turn all of the Queues into Arrays
        let valueSeries =
            ranges
            |> Seq.map (fun (KeyValue (value, ranges)) ->
                let rangeArray = ranges.ToArray()
                let newSeries = Series.ofRanges rangeArray
                KeyValuePair (value, newSeries))
            |> Dictionary
        
        {
            ValueSeries = valueSeries
            Values = bar<Units.ValueKey, _> values
        }

  
[<Struct>]
type SliceSetEnumerator<'T> =
    private {
        mutable CurKeyRangeIdx : int
        mutable CurValueKey : ValueKey
        mutable CurValueKeyBound : ValueKey
        mutable CurValue : 'T
        KeyRanges : Series<Units.ValueKey>
        Values : Bar<Units.ValueKey, 'T>
    }
    member e.MoveNext () =
        if e.CurValueKey < 0<_> && e.CurKeyRangeIdx < e.KeyRanges.Length  then
            e.CurValueKey <- e.KeyRanges.Starts[e.CurKeyRangeIdx]
            e.CurValueKeyBound <- e.KeyRanges.Bounds[e.CurKeyRangeIdx]
            e.CurValue <- e.Values[e.CurValueKey]
            true
        else
            e.CurValueKey <- e.CurValueKey + 1<_>
            if e.CurValueKey < e.CurValueKeyBound then
                e.CurValue <- e.Values[e.CurValueKey]
                true
            else
                e.CurKeyRangeIdx <- e.CurKeyRangeIdx + 1
                if e.CurKeyRangeIdx < e.KeyRanges.Length then
                    e.CurValueKey <- e.KeyRanges.Starts[e.CurKeyRangeIdx]
                    e.CurValueKeyBound <- e.KeyRanges.Bounds[e.CurKeyRangeIdx]
                    e.CurValue <- e.Values[e.CurValueKey]
                    true
                else
                    false
                
    member e.Current : 'T =
        if e.CurValueKey < 0<_> then
            raise (InvalidOperationException "Enumeration has not started. Call MoveNext.")
        else
            e.CurValue
        
[<Struct>]
type SliceSet<'a when 'a : equality>(
    keyRanges: Series<Units.ValueKey>,
    index: ValueIndex<'a>
    ) =
    
    member _.GetEnumerator () =
        {
            CurKeyRangeIdx = 0
            CurValueKey = -1<_>
            CurValueKeyBound = 0<_>
            CurValue = Unchecked.defaultof<'a>
            KeyRanges = keyRanges
            Values = index.Values
        }
        
    member s.AsSeq () =
        let mutable e = s.GetEnumerator()
        Seq.unfold (fun _ ->
            if e.MoveNext() then
                Some (e.Current, ())
            else
                None
            ) ()

            
[<Struct>]
type SliceSet3D<'a, 'b, 'c when 'a : equality and 'b : equality and 'c : equality>(
    keyRanges: Series<Units.ValueKey>,
    aIndex: ValueIndex<'a>,
    bIndex: ValueIndex<'b>,
    cIndex: ValueIndex<'c>
    ) =
    
    new (values: array<'a * 'b * 'c>) =
        let aValues = values |> Array.map (fun (a, _, _) -> a)
        let bValues = values |> Array.map (fun (_, b, _) -> b)
        let cValues = values |> Array.map (fun (_, _, c) -> c)
        let aIndex = ValueIndex.create aValues
        let bIndex = ValueIndex.create bValues
        let cIndex = ValueIndex.create cValues
        let keyFilter = Series.all values.Length
        SliceSet3D (keyFilter, aIndex, bIndex, cIndex)
        
    new (values: list<'a * 'b * 'c>) =
        let values = Array.ofList values
        SliceSet3D values
        
    member _.Item
        with get (aKey: 'a, bKey: 'b,  _: All) =
            
            let aSeries =
                match aIndex.ValueSeries.TryGetValue aKey with
                | true, s -> s
                | false, _ -> Series.empty
                
            let bSeries =
                match bIndex.ValueSeries.TryGetValue bKey with
                | true, s -> s
                | false, _ -> Series.empty

            let newKeyRanges =
                keyRanges
                |> Series.intersect aSeries
                |> Series.intersect bSeries
            
            SliceSet (newKeyRanges, cIndex)

            
    member _.Item
        with get (aKey: 'a,  _: All, cKey: 'c) =
            
            let aSeries =
                match aIndex.ValueSeries.TryGetValue aKey with
                | true, s -> s
                | false, _ -> Series.empty
                
            let cSeries =
                match cIndex.ValueSeries.TryGetValue cKey with
                | true, s -> s
                | false, _ -> Series.empty

            let newKeyRanges =
                keyRanges
                |> Series.intersect aSeries
                |> Series.intersect cSeries
            
            SliceSet (newKeyRanges, bIndex)
            
    member _.Item
        with get (_: All, bKey: 'b, cKey: 'c) =
            
            let bSeries =
                match bIndex.ValueSeries.TryGetValue bKey with
                | true, s -> s
                | false, _ -> Series.empty
                
            let cSeries =
                match cIndex.ValueSeries.TryGetValue cKey with
                | true, s -> s
                | false, _ -> Series.empty

            let newKeyRanges =
                keyRanges
                |> Series.intersect bSeries
                |> Series.intersect cSeries
            
            SliceSet (newKeyRanges, aIndex)

