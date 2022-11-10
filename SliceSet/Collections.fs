module SliceSet.Collections

open System

type Math () =
    
    static member inline min (a: int<'Measure>, b: int<'Measure>) =
        Math.Min (int a, int b)
        |> LanguagePrimitives.Int32WithMeasure<'Measure>

    static member inline max (a: int<'Measure>, b: int<'Measure>) =
        Math.Max (int a, int b)
        |> LanguagePrimitives.Int32WithMeasure<'Measure>


[<Struct>]
type Block<'T>(values: 'T[]) =
    // WARNING: Public for inlining only
    member _._values = values
    member b.Item
        with inline get k = b._values[k]
    member inline b.Length = b._values.Length
    member _.ToArray () = Array.copy values
    
    member _.Append (v: 'T) =
        let newValues = Array.zeroCreate (values.Length + 1)
        values.CopyTo(newValues, 0)
        newValues[newValues.Length - 1] <- v
        Block newValues
        
    member _.Concat (addValues: 'T[]) =
        let newValues = Array.zeroCreate (values.Length + addValues.Length)
        values.CopyTo (newValues, 0)
        addValues.CopyTo (newValues, values.Length)
        Block newValues

type block<'T> = Block<'T>


[<Struct>]
type Bar<[<Measure>] 'Measure, 'T> (values: 'T[]) =
    // WARNING: Not for public consumption
    member _._values = values
    member b.Item
        with inline get (k: int<'Measure>) = b._values[int k]
    member inline b.Length = LanguagePrimitives.Int32WithMeasure<'Measure> b._values.Length

type bar<[<Measure>] 'Measure, 'T> = Bar<'Measure, 'T>


[<Struct>]
type Row<[<Measure>] 'Measure, 'T> (values: 'T[]) =
    // WARNING: Not for public consumption
    member _._values = values
    member b.Item
        with inline get (k: int<'Measure>) = b._values[int k]
        and inline set (k: int<'Measure>) v = b._values[int k] <- v
    member inline b.Length = LanguagePrimitives.Int32WithMeasure<'Measure> b._values.Length

type row<[<Measure>] 'Measure, 'T> = Bar<'Measure, 'T>