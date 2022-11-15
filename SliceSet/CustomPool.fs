namespace SliceSet.CustomPool


open System
open System.Buffers
open System.Collections.Generic
open System.Numerics
open SliceSet.Collections
open SliceSet.Domain


[<Struct>]
type Range =
    {
        Start : int<Units.ValueKey>
        // This is the EXCLUSIVE upper bound
        Bound : int<Units.ValueKey>
    }
     
type Series = Range[]

module Series =
    
    module private RangePool =
        
        let pools = Dictionary<int, Stack<Range[]>>()

        let computeSizeKey x =
            // We have to subtract 1 from the input to ensure that values on
            // the bound of powers of 2 don't move up a power
            let leadingZero = BitOperations.LeadingZeroCount (uint (x - 1))
            1 <<< (32 - leadingZero)
        
        let rent (minSize: int) =
            let sizeKey = computeSizeKey minSize
            
            match pools.TryGetValue sizeKey with
            | true, pool ->
                if pool.Count > 0 then
                    pool.Pop()
                else
                    Array.zeroCreate sizeKey
                                
            | false, _ ->
                // Create a new array to return
                Array.zeroCreate sizeKey
                
            
        let restore (arr: Range[]) =
            let sizeKey = computeSizeKey arr.Length
            
            match pools.TryGetValue sizeKey with
            | true, pool -> pool.Push arr
            | false, _ ->
                let newPool = Stack()
                newPool.Push arr
                pools[sizeKey] <- newPool

        
    let all (length: int) =
        [| { Start = 0<_>; Bound = length * 1<_> } |]
    
    let empty =
        [| { Start = 0 * 1<_>; Bound = 0 * 1<_> } |]
    
    let intersect (a: Series) (b: Series) : Series =
        if a.Length = 0 || b.Length = 0 then
            empty
            
        else
            let resultAcc = RangePool.rent (a.Length + b.Length)
            let mutable aIdx = 0
            let mutable bIdx = 0
            let mutable resultIdx = 0
            let mutable aRange = a[aIdx]
            let mutable bRange = b[bIdx]
            
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
            RangePool.restore resultAcc
            result

type ValueIndex<'T> =
    {
        ValueSeries : Dictionary<'T, Series>
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
        KeyRanges : Series
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
    keyRanges: Series,
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
    keyRanges: Series,
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

