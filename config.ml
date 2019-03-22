(* ****************************** *)
(*     Simulation parameters      *)
(* ****************************** *)

(* Number of tolerable faulty nodes *)
let f = 1;;

(* Number of shards *)
let n_shards = 3;;

(* Max messages to be dropped per round *)
let max_drop = 1;;

(* 1/this chance for 1 message to be dropped *)
let drop_chance = 3;;

(* Max transactions to be addded per round of schedule *)
let max_transactions = 3;;

(* 1/this chance for 1 transaction to be added *)
let add_chance = 5;;

(* 1/this chance for a schedule element to be valid *)
let valid_chance = 2

(* Max objects to be added in a transaction *)
let max_obj_in = 5;;
let max_obj_out = 5;;
let obj_in_chance = 3;;
let obj_out_chance = 3;;

(* Byzantine behavior mode:
   0. Faulty replicas will send Commits instead of Aborts
      and Aborts instead of Commits
   1. Faulty replicas just send random stuff
 *)
let byz_mode = 0;;
