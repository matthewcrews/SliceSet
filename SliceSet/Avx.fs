namespace SliceSet.Avx

open System
open System.Buffers
open System.Collections.Generic
open System.Runtime.Intrinsics.X86
open System.Runtime.Intrinsics
open Microsoft.FSharp.NativeInterop
open SliceSet.Collections
open SliceSet.Domain

#nowarn "9"
#nowarn "42"

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
            
            let startsAcc = ArrayPool.Shared.Rent (a.Length + b.Length)
            let boundsAcc = ArrayPool.Shared.Rent (a.Length + b.Length)
            let aStarts : int[] = Helpers.retype a.Starts
            let bStarts : int[] = Helpers.retype b.Starts
            let aBounds : int[] = Helpers.retype a.Bounds
            let bBounds : int[] = Helpers.retype b.Bounds
            
            let mutable aIdx = 0
            let mutable bIdx = 0
            
            let mutable resultIdx = 0
            let mutable aRange = Unchecked.defaultof<_>
            let mutable bRange = Unchecked.defaultof<_>
            
            // We only want to use this loop if the b series is long enough
            // for us to take advantage of the AVX instructions
            if b.Length > Vector256<int>.Count then
                let lastBlockIdx = b.Length - (b.Length % Vector256<int>.Count)
                let aStartsPtr = && aStarts.AsSpan().GetPinnableReference()
                let aBoundsPtr = && aBounds.AsSpan().GetPinnableReference()
                let bStartsPtr = && bStarts.AsSpan().GetPinnableReference()
                let bBoundsPtr = && bBounds.AsSpan().GetPinnableReference()
                
                while bIdx < lastBlockIdx && aIdx < a.Length do
                    
                    if bBounds[bIdx + 7] <= aStarts[aIdx] then
                        bIdx <- bIdx + Vector256<int>.Count
                    elif aBounds[aIdx] <= bStarts[bIdx] then
                        aIdx <- aIdx + 1
                    else
                        // Broadcast a values into a Vector256
                        let aStartVec = Avx2.BroadcastScalarToVector256 (NativePtr.add aStartsPtr aIdx)
                        let aBoundVec = Avx2.BroadcastScalarToVector256 (NativePtr.add aBoundsPtr aIdx)
                        // Load the b values
                        let bStartsVec = Avx2.LoadVector256 (NativePtr.add bStartsPtr bIdx)
                        let bBoundsVec = Avx2.LoadVector256 (NativePtr.add bBoundsPtr bIdx)
                        
                        // Compute the possible Starts and Bounds
                        let newStarts = Avx2.Max (aStartVec, bStartsVec)
                        let newBounds = Avx2.Min (aBoundVec, bBoundsVec)
                        // Check if the computer ranges make sense. Negative values are non-overlaps
                        let boundsGreaterThanStarts = Avx2.CompareGreaterThan (newBounds, newStarts)
                        
                        // Move the farther behind series forward
                        if aBounds[aIdx] < bBounds[bIdx + 7] then
                            aIdx <- aIdx + 1
                        else
                            bIdx <- bIdx + Vector256<int>.Count
                        
            // Cleanup loop for end of the b series
            while aIdx < a.Length && bIdx < b.Length do
            
                if bBounds[bIdx] <= aStarts[aIdx] then
                    bIdx <- bIdx + 1
                elif aBounds[aIdx] <= bStarts[bIdx] then
                    aIdx <- aIdx + 1
                else
                    let newStart = Math.max (aStarts[aIdx], bStarts[bIdx])
                    let newBound = Math.min (aBounds[aIdx], bBounds[bIdx])
                    startsAcc[resultIdx] <- newStart
                    boundsAcc[resultIdx] <- newBound
                    resultIdx <- resultIdx + 1
                        
                    if aRange.Bound < bRange.Bound then
                        aIdx <- aIdx + 1
                    else
                        bIdx <- bIdx + 1
                        
            // Copy the final results
            let resultStarts = GC.AllocateUninitializedArray resultIdx
            let resultBounds = GC.AllocateUninitializedArray resultIdx
            Array.Copy (startsAcc, resultStarts, resultIdx)
            Array.Copy (boundsAcc, resultBounds, resultIdx)
            // Return the rented array
            ArrayPool.Shared.Return (startsAcc, false)
            ArrayPool.Shared.Return (boundsAcc, false)
            {
                Starts = resultStarts
                Bounds = resultBounds
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
                KeyValuePair (value, rangeArray))
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
            let curRange = e.KeyRanges[e.CurKeyRangeIdx]
            e.CurValueKey <- curRange.Start
            e.CurValueKeyBound <- curRange.Bound
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
                    let curRange = e.KeyRanges[e.CurKeyRangeIdx]
                    e.CurValueKey <- curRange.Start
                    e.CurValueKeyBound <- curRange.Bound
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

