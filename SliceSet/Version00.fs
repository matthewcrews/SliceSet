module SliceSet.Version00

open SliceSet.Domain


[<Struct>]
type SliceSet3D<'a, 'b, 'c when 'a : equality and 'b : equality and 'c : equality>(
    values: ('a * 'b * 'c)[]
    ) =
        
    member _.Item
        with get (aKey: 'a, bKey: 'b,  _: All) =
            values
            |> Array.filter (fun (a, b, _) -> a = aKey && b = bKey)
            |> Array.map (fun (_, _, c) -> c)
            
    member _.Item
        with get (aKey: 'a,  _: All, cKey: 'c) =
            values
            |> Array.filter (fun (a, _, c) -> a = aKey && c = cKey)
            |> Array.map (fun (_, b, _) -> b)
            
    member _.Item
        with get (_: All, bKey: 'b, cKey: 'c) =
            values
            |> Array.filter (fun (_, b, c) -> b = bKey && c = cKey)
            |> Array.map (fun (a, _, _) -> a)
                

