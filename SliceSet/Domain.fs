module SliceSet.Domain

[<Struct>]
type All = All

module Units =

    [<Measure>] type ValueKey
    [<Measure>] type RangeKey
    [<Measure>] type Product
    [<Measure>] type Supplier
    [<Measure>] type Customer

type ValueKey = int<Units.ValueKey>
type RangeKey = int<Units.RangeKey>
type Product = int<Units.Product>
type Supplier = int<Units.Supplier>
type Customer = int<Units.Customer>


module Product =
    
    let create (p: int) : Product =
        if p < 0 then
            invalidArg (nameof p) "Cannot have Product with negative value"
            
        p * 1<_>
        
module Supplier =
    
    let create (s: int) : Supplier =
        if s < 0 then
            invalidArg (nameof s) "Cannot have Supplier with negative value"
            
        s * 1<_>
        
module Customer =
    
    let create (c: int) : Customer =
        if c < 0 then
            invalidArg (nameof c) "Cannot have Customer with negative value"
            
        c * 1<_>
    