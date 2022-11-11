namespace SliceSet.Version02

open System
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
    
    // [<GeneralizableValue>]
    let empty<[<Measure>] 'Measure> =
        [| { Start = LanguagePrimitives.Int32WithMeasure<'Measure> 0; Bound = LanguagePrimitives.Int32WithMeasure<'Measure> 0 } |]
    
    let intersect (a: Series<'Measure>) (b: Series<'Measure>) : Series<'Measure> =
        if a.Length = 0 || b.Length = 0 then
            Array.empty
            
        else
            let mutable aIdx = 0
            let mutable bIdx = 0
            let mutable aRange = a[aIdx]
            let mutable bRange = b[bIdx]
            let result = Queue()
            while aIdx < a.Length && bIdx < b.Length do
        
                if aRange.Start <=bRange.Bound && bRange.Start <= aRange.Bound then
                    let newStart = Math.max (aRange.Start, bRange.Start)
                    let newBound = Math.min (aRange.Bound, bRange.Bound)
                    let newRange = { Start = newStart; Bound = newBound }
                    result.Enqueue newRange
                    
                if aRange.Bound < bRange.Bound then
                    aIdx <- aIdx + 1
                    aRange <- a[aIdx]
                else
                    bIdx <- bIdx + 1
                    bRange <- b[bIdx]
        
            result.ToArray()

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
        mutable _curKeyRangeIdx : int
        mutable _curValueKey : ValueKey
        mutable _curValueKeyBound : ValueKey
        mutable _curValue : 'T
        _keyRanges : Series<Units.ValueKey>
        _values : Bar<Units.ValueKey, 'T>
    }
    member e.MoveNext () =
        if e._curValueKey < e._curValueKeyBound then
            e._curValueKey <- e._curValueKey + 1<_>
            e._curValue <- e._values[e._curValueKey]
            true
        else
            if e._curKeyRangeIdx < e._keyRanges.Length then
                e._curKeyRangeIdx <- e._curKeyRangeIdx + 1
                let curRange = e._keyRanges[e._curKeyRangeIdx]
                e._curValueKey <- curRange.Start
                e._curValueKeyBound <- curRange.Bound
                e._curValue <- e._values[e._curValueKey]
                true
            else
                false
                
    member e.Current : 'T =
        if e._curValueKey < 0<_> then
            raise (InvalidOperationException "Enumeration has not started. Call MoveNext.")
        else
            e._curValue
        
[<Struct>]
type SliceSet<'a when 'a : equality>(
    keyRanges: Series<Units.ValueKey>,
    index: ValueIndex<'a>
    ) =
    
    member _.GetEnumerator () =
        {
            _curKeyRangeIdx = 0
            _curValueKey = -1<_>
            _curValueKeyBound = 0<_>
            _curValue = Unchecked.defaultof<'a>
            _keyRanges = keyRanges
            _values = index.Values
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
            
            SliceSet (newKeyRanges, cIndex)
            
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
            
            SliceSet (newKeyRanges, cIndex)

