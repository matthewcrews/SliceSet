namespace SliceSet.Branchless

open System
open System.Buffers
open System.Collections.Generic
open SliceSet.Collections
open SliceSet.Domain

#nowarn "42"

[<Struct>]
type Range<[<Measure>] 'Measure> =
    {
        Start : int<'Measure>
        // This is the EXCLUSIVE upper bound
        Bound : int<'Measure>
    }
     
type Series<[<Measure>] 'Measure> = Range<'Measure>[]

module Series =
    
    let inline private retype<'T,'U> (x: 'T) : 'U = (# "" x: 'U #)
    
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
        
                let bLessThanA = bRange.Bound <= aRange.Start
                let aLessThanB = aRange.Bound <= bRange.Start
                let test = bLessThanA || aLessThanB
                
                if test then
                    bIdx <- bIdx + (retype bLessThanA)
                    aIdx <- aIdx + (retype aLessThanB)
                else
                    let aStartGreater = retype (aRange.Start > bRange.Start)
                    let newStart = aRange.Start * aStartGreater + bRange.Start * (1 - aStartGreater)
                    let aBoundLess = retype (aRange.Bound < bRange.Bound)
                    let newBound = aRange.Bound * aBoundLess + bRange.Bound * (1 - aBoundLess)
                    let newRange = { Start = newStart; Bound = newBound }
                    resultAcc[resultIdx] <- newRange
                    resultIdx <- resultIdx + 1
                        
                    let aBoundLessThanB = retype (aRange.Bound < bRange.Bound)
                    aIdx <- aIdx + aBoundLessThanB
                    bIdx <- bIdx + (1 - aBoundLessThanB)
                        
            // Copy the final results
            let result = GC.AllocateUninitializedArray resultIdx
            Array.Copy (resultAcc, result, resultIdx)
            // Return the rented array
            ArrayPool.Shared.Return (resultAcc, false)
            result

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

