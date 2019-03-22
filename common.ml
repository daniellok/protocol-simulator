type timestamp = int;;
type address   = int;;
type id        = int;;
type decision =
  | Commit
  | Abort;;

type obj = {
    id     : id;
    owner  : id;
    value  : int;
  };;

let obj n = {
    id = n;
    owner = 0;
    value = 10;
  };;

let rec pop (l : 'a list) (n : int) : 'a list =
  match l with
  | []     -> []
  | h :: t -> if n = 0
              then t
              else h :: pop t (n - 1);;

let tl_with_default (l : 'a list) =
  match l with
  | [] -> []
  | h :: t -> t;;

(* remove Nones and convert Somes to messages *) 
let rec remove_options (xs : 'a option list) : 'a list =
  match xs with
  | [] ->
     []
  | x :: xs' ->
     match x with
     | Some a ->
        a :: remove_options xs'
     | None ->
        remove_options xs';;

let append_uniq (xs_init : 'a list) (ys_init : 'a list) : 'a list =
  let rec traverse xs ys =
    match xs with
    | [] ->
       ys
    | x :: xs' ->
       if List.mem x ys
       then traverse xs' ys
       else traverse xs' (x :: ys)
  in
  traverse xs_init ys_init;;
       
exception Request_not_in_log of string;;
exception Wrong_message_type of string;;
exception Transaction_not_found;;
