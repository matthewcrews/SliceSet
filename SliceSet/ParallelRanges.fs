namespace SliceSet.ParallelRanges

open System
open System.Collections.Generic
open SliceSet.Collections
open SliceSet.Domain


[<Struct>]
type ValueRange =
    {
        Start : ValueKey
        Length : ValueKey
    }

          
[<Struct>]
type SliceIndex<'T> = {
    StartRange : Dictionary<'T, RangeKey>
    Ranges : Bar<Units.RangeKey, ValueRange>
    NextRange : Bar<Units.RangeKey, RangeKey>
    Values : Bar<Units.ValueKey, 'T>
}
          
module SliceIndex =
    
    let create (values: 'T[]) =
        let startRange = Dictionary()
        let ranges = Queue()
        let prevRangeKeys = Dictionary()
        let nextRangeKeys = Dictionary()
        let mutable value = values[0]
        let mutable start = 0<Units.ValueKey>
        let mutable length = 0<Units.ValueKey>
        let mutable rangeKey = 0<Units.RangeKey>

        for i = 0 to values.Length - 1 do
            let valueKey = i * 1<Units.ValueKey>
            
            if values[i] = value then
                length <- length + 1<_>
            else
                // Create the new range and add it to the Queue
                let range = { Start = start; Length = length }
                ranges.Enqueue range
                
                // We check if we have entered a starting range for
                // this value or not. If we have not, we need to store it
                if not (startRange.ContainsKey value) then
                    startRange[value] <- rangeKey
                    
                // If we have seen it before, then we need to record an entry
                // for the NextRange Bar
                else
                    let prevRangeKey = prevRangeKeys[value]
                    nextRangeKeys[prevRangeKey] <- rangeKey
                    
                // Record that this is the last RangeKey for this value
                prevRangeKeys[value] <- rangeKey

                // Reset the mutable values
                value <- values[i]
                start <- valueKey
                length <- 1<_>
                rangeKey <- rangeKey + 1<_>


        // Wrap up the last Range the loop was working on
        // Create the new range and add it to the Queue
        let range = { Start = start; Length = length }
        ranges.Enqueue range
        
        // We check if we have entered a starting range for
        // this value or not. If we have not, we need to store it
        if not (startRange.ContainsKey value) then
            startRange[value] <- rangeKey
            
        // If we have seen it before, then we need to record an entry
        // for the NextRange Bar
        else
            let prevRangeKey = prevRangeKeys[value]
            nextRangeKeys[prevRangeKey] <- rangeKey
        

        // Create the final collections for the Index
        let ranges =
            let r = ranges.ToArray()
            Bar<Units.RangeKey, _> r

        let nextRange =
            let r = Array.zeroCreate (int ranges.Length)
            for KeyValue (rangeKey, nextRangeKey) in nextRangeKeys do
                r[int rangeKey] <- nextRangeKey
            Bar<Units.RangeKey, _> r


        let values = Bar<Units.ValueKey, _> values

        {
            StartRange = startRange
            Ranges = ranges
            NextRange = nextRange
            Values = values
        }


[<Struct>]
type KeyFilter =
    {
        IndexRanges: block<Bar<Units.RangeKey, ValueRange>>
        NextRanges: block<Bar<Units.RangeKey, RangeKey>>
        StartRanges: block<RangeKey>
    }
    static member empty (length: int) =
        let length = length * 1<_>
        {
            IndexRanges = block [| Bar<_,_> [| {Start = 0<_>; Length = length} |] |]
            NextRanges = block [| Bar<_,_> [|0<_>|] |]
            StartRanges = block [| 0<_> |]
        }

[<Struct>]
type IndexGroupIterator =
    {
        KeyFilter : KeyFilter
        CurRangeKeys: array<RangeKey>
        MinKeys: array<ValueKey>
        MaxKeys: array<ValueKey>
        mutable CurValueKey: ValueKey
        mutable CurMinValueKey: ValueKey
        mutable CurMaxValueKey: ValueKey
    }
    static member ofKeyFilter (kf: KeyFilter) : IndexGroupIterator =
        let curRangeKeys = kf.StartRanges.ToArray()
        let minKeys = Array.create kf.IndexRanges.Length 0<_>
        let maxKeys = Array.create kf.IndexRanges.Length 0<_>
        {
            KeyFilter = kf
            CurRangeKeys = curRangeKeys
            MinKeys = minKeys
            MaxKeys = maxKeys
            CurValueKey = -1<_>
            CurMinValueKey = 0<_>
            CurMaxValueKey = Int32.MaxValue * 1<_>
        }
        
    // We are moving the ranges forward
    member private x.Seek() : bool =
        // Tracking whether we are still seeing new ranges.
        let mutable result = true
        
        // Move all the ranges forward whose MaxValueKey is equal to CurMaxValueKey
        for i = 0 to x.MaxKeys.Length - 1 do
            if x.MaxKeys[i] = x.CurMaxValueKey then
                let curRangeKey = x.CurRangeKeys[i]
                let nextRangeKey = x.KeyFilter.NextRanges[i][curRangeKey]
                // We only want to perform this update if we are moving forward
                // in the index. If we are moving back, we have reached the end.
                if nextRangeKey > curRangeKey then
                    x.CurRangeKeys[i] <- nextRangeKey
                    // Get the new CurRange for the Index
                    let curRange = x.KeyFilter.IndexRanges[i][nextRangeKey]
                    // Update the Min and Max Value Key for the new Range
                    x.MinKeys[i] <- curRange.Start
                    x.MaxKeys[i] <- curRange.Start + curRange.Length - 1<_>
                else
                    result <- false
                    
                    
        // Check whether we are still making progress before re-evaluating. result
        // will be FALSE in the case one of the SliceIndexes no longer has ranges.
        if result then
            
            for minKey in x.MinKeys do
                x.CurMinValueKey <- Math.max (x.CurMinValueKey, minKey)
                
            // Reset the MaxValue
            x.CurMaxValueKey <- Int32.MaxValue * 1<_>
            for maxKey in x.MaxKeys do
                x.CurMaxValueKey <- Math.min (x.CurMaxValueKey, maxKey)
                
            if x.CurMinValueKey <= x.CurMaxValueKey then
                // We have found a new range of overlaps
                x.CurValueKey <- x.CurMinValueKey
            else
                // The ranges still do not overlap so we need to seek again
                result <- x.Seek()
                
        result
    
    member private x.Initialize() : bool =
        let minKeys = x.MinKeys
        let maxKeys = x.MaxKeys
        let indexRanges = x.KeyFilter.IndexRanges
        let curRangeKeys = x.CurRangeKeys
        
        for i = 0 to minKeys.Length - 1 do
            let ranges = indexRanges[i]
            let curRangeKey = curRangeKeys[i]
            let curRange = ranges[curRangeKey]
            minKeys[i] <- curRange.Start
            maxKeys[i] <- curRange.Start + curRange.Length - 1<_>
            x.CurMinValueKey <- Math.max (x.CurMinValueKey, minKeys[i])
            x.CurMaxValueKey <- Math.min (x.CurMaxValueKey, maxKeys[i])
            
        if x.CurMinValueKey <= x.CurMaxValueKey then
            x.CurValueKey <- x.CurMinValueKey
            true
        else
            x.Seek()
    
    member x.MoveNext () : bool =
        if x.CurValueKey < 0<_> then
            x.Initialize()
        elif x.CurValueKey < x.CurMaxValueKey then
            x.CurValueKey <- x.CurValueKey + 1<_>
            true
        else
            x.Seek()
        
    member x.Current = x.CurValueKey
  
  
[<Struct>]
type ValueIterator<'T> =
    {
        mutable IndexSet : IndexGroupIterator
        ValueLookup : ValueKey -> 'T
    }
    
    member x.MoveNext () = x.IndexSet.MoveNext()

    member x.Current =
        if x.IndexSet.CurValueKey < 0<_> then
            raise (InvalidOperationException "Enumeration has not started. Call MoveNext.")
        else
            x.ValueLookup x.IndexSet.CurValueKey

        
        
[<Struct>]
type SliceSet<'a when 'a : equality>(
    keyFilter: KeyFilter,
    index: SliceIndex<'a>
    ) =
    
    member _.GetEnumerator () =
        let indexSet = IndexGroupIterator.ofKeyFilter keyFilter
        let values = index.Values
        let lookup = fun k -> values[k]
        {
            IndexSet = indexSet
            ValueLookup = lookup
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
    keyFilter: KeyFilter,
    aIndex: SliceIndex<'a>,
    bIndex: SliceIndex<'b>,
    cIndex: SliceIndex<'c>
    ) =
    
    new (values: array<'a * 'b * 'c>) =
        let aValues = values |> Array.map (fun (a, _, _) -> a)
        let bValues = values |> Array.map (fun (_, b, _) -> b)
        let cValues = values |> Array.map (fun (_, _, c) -> c)
        let aIndex = SliceIndex.create aValues
        let bIndex = SliceIndex.create bValues
        let cIndex = SliceIndex.create cValues
        let keyFilter = KeyFilter.empty values.Length
        SliceSet3D (keyFilter, aIndex, bIndex, cIndex)
        
    new (values: list<'a * 'b * 'c>) =
        let values = Array.ofList values
        SliceSet3D values
        
    member _.Item
        with get (aKey: 'a, bKey: 'b,  _: All) =
            match aIndex.StartRange.TryGetValue aKey, bIndex.StartRange.TryGetValue bKey with
            | (true, aStartRangeKey), (true, bStartRangeKey) ->
                let indexRanges = keyFilter.IndexRanges.Concat [|aIndex.Ranges; bIndex.Ranges|]
                let nextRanges = keyFilter.NextRanges.Concat [|aIndex.NextRange; bIndex.NextRange|]
                let startRanges = keyFilter.StartRanges.Concat [|aStartRangeKey; bStartRangeKey|]
                let keyFilter : KeyFilter = {
                    IndexRanges = indexRanges
                    NextRanges = nextRanges
                    StartRanges = startRanges
                }
                SliceSet (keyFilter, cIndex)
                
            | (_, _), (_, _) ->
                raise (KeyNotFoundException "Index does not contain the value")
            
    member _.Item
        with get (aKey: 'a,  _: All, cKey: 'c) =
            match aIndex.StartRange.TryGetValue aKey, cIndex.StartRange.TryGetValue cKey with
            | (true, aStartRangeKey), (true, cStartRangeKey) ->
                let indexRanges = keyFilter.IndexRanges.Concat [|aIndex.Ranges; cIndex.Ranges|]
                let nextRanges = keyFilter.NextRanges.Concat [|aIndex.NextRange; cIndex.NextRange|]
                let startRanges = keyFilter.StartRanges.Concat [|aStartRangeKey; cStartRangeKey|]
                let keyFilter : KeyFilter = {
                    IndexRanges = indexRanges
                    NextRanges = nextRanges
                    StartRanges = startRanges
                }
                SliceSet (keyFilter, bIndex)
                
            | (_, _), (_, _) ->
                raise (KeyNotFoundException "Index does not contain the value")
            
    member _.Item
        with get (_: All, bKey: 'b, cKey: 'c) =
            match bIndex.StartRange.TryGetValue bKey, cIndex.StartRange.TryGetValue cKey with
            | (true, bStartRangeKey), (true, cStartRangeKey) ->
                let indexRanges = keyFilter.IndexRanges.Concat [|bIndex.Ranges; cIndex.Ranges|]
                let nextRanges = keyFilter.NextRanges.Concat [|bIndex.NextRange; cIndex.NextRange|]
                let startRanges = keyFilter.StartRanges.Concat [|bStartRangeKey; cStartRangeKey|]
                let keyFilter : KeyFilter = {
                    IndexRanges = indexRanges
                    NextRanges = nextRanges
                    StartRanges = startRanges
                }
                SliceSet (keyFilter, aIndex)
                
            | (_, _), (_, _) ->
                raise (KeyNotFoundException "Index does not contain the value")
