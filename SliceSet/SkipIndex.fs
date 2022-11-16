namespace SliceSet.SkipIndex

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
     
[<Struct>]
type Series<[<Measure>] 'Measure> =
    {
        Skips : int<'Measure>[]
        Ranges : Range<'Measure>[]
    }

module Series =
    
    let all (length: int) =
        let ranges = [| { Start = 0<_>; Bound = length * 1<_> } |]
        let skips = [|length * 1<_> |]
        {
            Skips = skips
            Ranges = ranges
        }
    
    let empty<[<Measure>] 'Measure> =
        let ranges = [| { Start = LanguagePrimitives.Int32WithMeasure<'Measure> 0; Bound = LanguagePrimitives.Int32WithMeasure<'Measure> 0 } |]
        let skips = [| LanguagePrimitives.Int32WithMeasure<'Measure> 0 |]
        {
            Skips = skips
            Ranges = ranges
        }
    
    let findFirstIndexForBound (curIdx: int) (boundTarget: int<_>)  (series: Series<'Measure>) =
        let ranges = series.Ranges
        let skipBounds = series.Skips
        let mutable resultIdx = curIdx
        let mutable skipIdx = resultIdx >>> 3

        if skipBounds[skipIdx] <= boundTarget then
            while skipIdx < skipBounds.Length - 1 && skipBounds[skipIdx] <= boundTarget do
                skipIdx <- skipIdx + 1
                
            resultIdx <- skipIdx <<< 3
            
        while resultIdx < ranges.Length - 1 && ranges[resultIdx].Bound <= boundTarget do
            resultIdx <- resultIdx + 1
        
        resultIdx
    
    
    let intersect (a: Series<'Measure>) (b: Series<'Measure>) : Series<'Measure> =
        if a.Ranges.Length = 0 || b.Ranges.Length = 0 then
            empty
            
        else
            let resultRangesAcc = ArrayPool.Shared.Rent (a.Ranges.Length + b.Ranges.Length)
            let resultSkipsAcc = ArrayPool.Shared.Rent (a.Ranges.Length + b.Ranges.Length)
            let mutable aIdx = 0
            let mutable bIdx = 0
            let mutable resultIdx = 0
            let aRanges = a.Ranges
            let aSkipBounds = a.Skips
            let bRanges = b.Ranges
            let bSkipBounds = b.Skips
            
            while aIdx < aRanges.Length && bIdx < bRanges.Length do
                // Check if B is behind A and seek forward if it is
                if bRanges[bIdx].Bound <= aRanges[aIdx].Start then
                    bIdx <- findFirstIndexForBound bIdx aRanges[aIdx].Start b

                // Check if A is behind B and seek forward if necessary
                if aRanges[aIdx].Bound <= bRanges[bIdx].Start then
                    aIdx <- findFirstIndexForBound aIdx bRanges[bIdx].Start a
                                
                // See if there is overlap and create a Range entry if there is                
                if aRanges[aIdx].Start < bRanges[bIdx].Bound then
                    let newStart = Math.max (aRanges[aIdx].Start, bRanges[bIdx].Start)
                    let newBound = Math.min (aRanges[aIdx].Bound, bRanges[bIdx].Bound)
                    let newRange = { Start = newStart; Bound = newBound }
                    resultRangesAcc[resultIdx] <- newRange
                    resultSkipsAcc[resultIdx >>> 3] <- newBound
                    resultIdx <- resultIdx + 1
                    
                if aRanges[aIdx].Bound < bRanges[bIdx].Bound then
                    aIdx <- aIdx + 1
                else
                    bIdx <- bIdx + 1
            
            
            // Copy the final results
            let resultRanges = GC.AllocateUninitializedArray resultIdx
            Array.Copy (resultRangesAcc, resultRanges, resultIdx)
            let resultSkipsCount = (resultIdx + 7) >>> 3
            let resultSkips = GC.AllocateUninitializedArray resultSkipsCount
            Array.Copy (resultSkipsAcc, resultSkips, resultSkipsCount)
            
            // Return the rented array
            ArrayPool.Shared.Return (resultRangesAcc, false)
            ArrayPool.Shared.Return (resultSkipsAcc, false)
            
            // Return the new Series
            {
                Skips = resultSkips
                Ranges = resultRanges
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

