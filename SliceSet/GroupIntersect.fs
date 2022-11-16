namespace SliceSet.GroupIntersect

open System
open System.Buffers
open System.Collections.Generic
open SliceSet.Collections
open SliceSet.Domain


[<Struct>]
type Range<[<Measure>] 'Measure> =
    {
        Start : int<'Measure>
        // This is the EXCLUSIVE upper bound
        Bound : int<'Measure>
    }
     
type Series<[<Measure>] 'Measure> = Range<'Measure>[]

module Series =
    
    let all (length: int) =
        [| { Start = 0<_>; Bound = length * 1<_> } |]
    
    let empty<[<Measure>] 'Measure> =
        [| { Start = LanguagePrimitives.Int32WithMeasure<'Measure> 0; Bound = LanguagePrimitives.Int32WithMeasure<'Measure> 0 } |]
    
    let intersect (a: Series<'Measure>) (b: Series<'Measure>) : Series<'Measure> =
        if a.Length = 0 || b.Length = 0 then
            Array.empty
            
        else
            let resultAcc = ArrayPool.Shared.Rent (a.Length + b.Length)
            let mutable aIdx = 0
            let mutable bIdx = 0
            let mutable resultIdx = 0
            let mutable aRange = Unchecked.defaultof<_>
            let mutable bRange = Unchecked.defaultof<_>
            
            while aIdx < a.Length && bIdx < b.Length do
                aRange <- a[aIdx]
                bRange <- b[bIdx]
        
                if bRange.Bound <= aRange.Start then
                    bIdx <- bIdx + 1
                elif aRange.Bound <= bRange.Start then
                    aIdx <- aIdx + 1
                else
                    let newStart = Math.max (aRange.Start, bRange.Start)
                    let newBound = Math.min (aRange.Bound, bRange.Bound)
                    let newRange = { Start = newStart; Bound = newBound }
                    resultAcc[resultIdx] <- newRange
                    resultIdx <- resultIdx + 1
                        
                    if aRange.Bound < bRange.Bound then
                        aIdx <- aIdx + 1
                    else
                        bIdx <- bIdx + 1
                        
            // Copy the final results
            let result = GC.AllocateUninitializedArray resultIdx
            Array.Copy (resultAcc, result, resultIdx)
            // Return the rented array
            ArrayPool.Shared.Return (resultAcc, false)
            result
            
    module Span =
        
        let inline exists ([<InlineIfLambda>] f) (s: Span<'T>) =
            let mutable result = false
            let mutable i = 0
            while i < s.Length && (not result) do
                result <- f s[i]
                i <- i + 1
                
            result
            
            
        let inline sumBy ([<InlineIfLambda>] f) (s: Span<'T>) =
            let mutable acc = LanguagePrimitives.GenericZero
            let mutable i = 0
            
            for x in s do
                acc <- acc + (f x)
                
            acc
            
            
    let intersectAll (seriesGroup: Span<Series<'Measure>>) : Series<'Measure> =
        if Span.exists (fun (s: Series<_>) -> s.Length = 0) seriesGroup then
            Array.empty
            
        else
            let resultLength = Span.sumBy (fun (s: Series<_>) -> s.Length) seriesGroup
            let resultAccArr : Range<'Measure>[] = ArrayPool.Shared.Rent resultLength
            let resultAcc = resultAccArr.AsSpan()
            let mutable resultIdx = 0
            let mutable progressing = true
            
            let curIndexesArr : int[] = ArrayPool.Shared.Rent seriesGroup.Length
            let curIndexes = curIndexesArr.AsSpan (0, seriesGroup.Length)
            let mutable curStart = LanguagePrimitives.Int32WithMeasure<'Measure> 0
            let mutable curBound = LanguagePrimitives.Int32WithMeasure<'Measure> Int32.MaxValue
                        
            // Setup the current Indexes, Starts, and Bounds
            for i in 0 .. seriesGroup.Length - 1 do
                curIndexes[i] <- 0
                let range = seriesGroup[i][0]
                if range.Start > curStart then
                    curStart <- range.Start
                    
                if range.Bound < curBound then
                    curBound <- range.Bound

            // Check that we are still making progress before continuing the loop
            while progressing do
                if curStart < curBound then
                    let newRange = { Start = curStart; Bound = curBound }
                    resultAcc[resultIdx] <- newRange
                    resultIdx <- resultIdx + 1
                        
                // Iterate through all of the ranges and move the ones forward
                // which are at the current Bound
                for i in 0 .. seriesGroup.Length - 1 do
                    let series = seriesGroup[i]
                    let range = series[curIndexes[i]]
                    if range.Bound = curBound then
                        curIndexes[i] <- curIndexes[i] + 1
                        // Check if we have reached the end of the series
                        if not (curIndexes[i] < series.Length) then
                            progressing <- false
                            
                        
                // Recompute the curStart and curBound
                if progressing then
                    curStart <- 0<_>
                    curBound <- LanguagePrimitives.Int32WithMeasure<'Measure> Int32.MaxValue
                    for i in 0 .. seriesGroup.Length - 1 do
                        let series = seriesGroup[i]
                        let curIndex = curIndexes[i]
                        let curRange = series[curIndex]
                        if curRange.Start > curStart then
                            curStart <- curRange.Start
                            
                        if curRange.Bound < curBound then
                            curBound <- curRange.Bound
                        
            // Copy the final results
            let resultArr = GC.AllocateUninitializedArray resultIdx
            let result = resultArr.AsSpan()
            resultAcc.Slice(0, resultIdx).CopyTo result
            // Return the rented arrays
            ArrayPool.Shared.Return (resultAccArr, false)
            ArrayPool.Shared.Return (curIndexesArr, false)
            // Return result array
            resultArr


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

            let seriesGroup = ArrayPool.Shared.Rent 3
            
            seriesGroup[0] <- keyRanges
            seriesGroup[1] <- aSeries
            seriesGroup[2] <- bSeries
            
            let newKeyRanges = Series.intersectAll (seriesGroup.AsSpan(0, 3))
            ArrayPool.Shared.Return (seriesGroup, false)
                        
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
            
            
            let seriesGroup = ArrayPool.Shared.Rent 3
            
            seriesGroup[0] <- keyRanges
            seriesGroup[1] <- aSeries
            seriesGroup[2] <- cSeries
            
            let newKeyRanges = Series.intersectAll (seriesGroup.AsSpan(0, 3))
            ArrayPool.Shared.Return (seriesGroup, false)
            
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

            let seriesGroup = ArrayPool.Shared.Rent 3
            
            seriesGroup[0] <- keyRanges
            seriesGroup[1] <- bSeries
            seriesGroup[2] <- cSeries
            
            let newKeyRanges = Series.intersectAll (seriesGroup.AsSpan(0, 3))
            ArrayPool.Shared.Return (seriesGroup, false)
            
            SliceSet (newKeyRanges, aIndex)

