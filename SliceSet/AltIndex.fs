namespace SliceSet.AltIndex

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
     
type RangeSeries<[<Measure>] 'Measure> = Range<'Measure>[]
type PointSeries<[<Measure>] 'Measure> = int<'Measure>[]

[<Struct; RequireQualifiedAccess>]
type Series<[<Measure>] 'Measure> =
    | Range of ranges: Range<'Measure>[]
    | Point of points: int<'Measure>[]


[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Series =
    
    let ofRanges (ranges: Range<_>[]) =
        
        if ranges |> Array.exists (fun r -> r.Bound > r.Start + 1<_>) then
            Series.Range ranges
        else
            let points =
                ranges
                |> Array.map (fun r -> r.Start)
            Series.Point points
        
    
    let all<[<Measure>] 'Measure> (length: int) =
        Series.Range [| { Start = LanguagePrimitives.Int32WithMeasure<'Measure> 0; Bound = LanguagePrimitives.Int32WithMeasure<'Measure> length } |]
    
    
    let empty<[<Measure>] 'Measure> : Series<'Measure> =
        Series.Point Array.empty
        
        
    module private Intersects =
        
        let rangeAndRange<[<Measure>] 'Measure> (a: RangeSeries<'Measure>) (b: RangeSeries<'Measure>) : Series<'Measure> =
            if a.Length = 0 || b.Length = 0 then
                empty
                
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
                Series.Range result
                
                
        let pointAndPoint (a: PointSeries<'Measure>) (b: PointSeries<'Measure>) : Series<'Measure> =
            if a.Length = 0 || b.Length = 0 then
                empty
            else
                let resultAcc = ArrayPool.Shared.Rent (a.Length + b.Length)
                let mutable aIdx = 0
                let mutable bIdx = 0
                let mutable resultIdx = 0
                
                while aIdx < a.Length && bIdx < b.Length do
            
                    if b[bIdx] < a[aIdx] then
                        bIdx <- bIdx + 1
                    elif a[aIdx] < b[bIdx] then
                        aIdx <- aIdx + 1
                    else
                        resultAcc[resultIdx] <- a[aIdx]
                        resultIdx <- resultIdx + 1
                            
                        if a[aIdx] < b[bIdx] then
                            aIdx <- aIdx + 1
                        else
                            bIdx <- bIdx + 1
                            
                // Copy the final results
                let result = GC.AllocateUninitializedArray resultIdx
                Array.Copy (resultAcc, result, resultIdx)
                // Return the rented array
                ArrayPool.Shared.Return (resultAcc, false)
                Series.Point result
        
        
        let rangeAndPoint<[<Measure>] 'Measure> (a: RangeSeries<'Measure>) (b: PointSeries<'Measure>) : Series<'Measure> =
            if a.Length = 0 || b.Length = 0 then
                empty
                
            else
                let resultAcc = ArrayPool.Shared.Rent (a.Length + b.Length)
                let mutable aIdx = 0
                let mutable bIdx = 0
                let mutable resultIdx = 0
                let mutable aRange = Unchecked.defaultof<_>
                
                while aIdx < a.Length && bIdx < b.Length do
                    aRange <- a[aIdx]
            
                    if b[bIdx] < aRange.Start then
                        bIdx <- bIdx + 1
                    elif aRange.Bound <= b[bIdx] then
                        aIdx <- aIdx + 1
                    else
                        resultAcc[resultIdx] <- b[bIdx]
                        resultIdx <- resultIdx + 1
                            
                        if aRange.Bound < b[bIdx] then
                            aIdx <- aIdx + 1
                        else
                            bIdx <- bIdx + 1
                            
                // Copy the final results
                let result = GC.AllocateUninitializedArray resultIdx
                Array.Copy (resultAcc, result, resultIdx)
                // Return the rented array
                ArrayPool.Shared.Return (resultAcc, false)
                Series.Point result
        
        
    let intersect (a: Series<'Measure>) (b: Series<'Measure>) : Series<'Measure> =
        
        match a, b with
        | Series.Range a, Series.Range b -> Intersects.rangeAndRange a b
        | Series.Range a, Series.Point b
        | Series.Point b, Series.Range a -> Intersects.rangeAndPoint a b
        | Series.Point a, Series.Point b -> Intersects.pointAndPoint a b
        

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
type SliceSetRangeEnumerator<'T> =
    private {
        mutable CurKeyRangeIdx : int
        mutable CurValueKey : ValueKey
        mutable CurValueKeyBound : ValueKey
        mutable CurValue : 'T
        KeyRanges : RangeSeries<Units.ValueKey>
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
type SliceSetPointEnumerator<'T> =
    private {
        mutable CurKey : int
        mutable CurValue : 'T
        Points : PointSeries<Units.ValueKey>
        Values : Bar<Units.ValueKey, 'T>
    }
    member e.MoveNext () =
        e.CurKey <- e.CurKey + 1
        if e.CurKey < e.Points.Length then
            e.CurValue <- e.Values[e.Points[e.CurKey]]
            true
        else
            false
            
    member e.Current : 'T =
        if e.CurKey < 0 then
            raise (InvalidOperationException "Enumeration has not started. Call MoveNext.")
        else
            e.CurValue
       
[<AbstractClass>]
type SliceSetEnumerator<'T> () =
    abstract member MoveNext : unit -> bool
    abstract member Current : 'T
            
            
[<Struct>]
type SliceSet<'a when 'a : equality>(
    keyRanges: Series<Units.ValueKey>,
    index: ValueIndex<'a>
    ) =
    
    member _.GetEnumerator () =
        match keyRanges with
        | Series.Range ranges ->
            let mutable s = {
                CurKeyRangeIdx = 0
                CurValueKey = -1<_>
                CurValueKeyBound = 0<_>
                CurValue = Unchecked.defaultof<'a>
                KeyRanges = ranges
                Values = index.Values
            }
            let e = { new SliceSetEnumerator<_>() with
                        member x.MoveNext () = s.MoveNext ()
                        member x.Current = s.Current }
            e
            
        | Series.Point points ->
            let mutable s = {
                CurKey = -1
                CurValue = Unchecked.defaultof<'a>
                Points = points
                Values = index.Values
            }
            let e = { new SliceSetEnumerator<_>() with
                        member x.MoveNext () = s.MoveNext ()
                        member x.Current = s.Current }
            e
        
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

