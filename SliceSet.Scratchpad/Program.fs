open SliceSet

// let test =
//     SliceSet2D [
//         1, 1
//         1, 2
//         1, 3
//         2, 5
//         2, 10
//         7, 1
//         8, 1
//         1, 4
//         1, 5
//         8, 2
//         1, 6
//     ]
//     
// let x = test[1, All]
//
// for a in x do
//     printfn $"{a}"
    
let t2 =
    SliceSet3D [
        1, 1, "a"
        1, 2, "b"
        1, 3, "c"
        2, 1, "aa"
        2, 2, "bb"
        2, 3, "cc"
        3, 3, "dd"
    ]

let x2 = t2[All, 2, All]
// let x3 = x2[2, All]

for a in x2 do
    printfn $"{a}"
    
()