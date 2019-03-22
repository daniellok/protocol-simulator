#use "sbac.ml";;
#print_length 99999;;

(* ****************************** *)
(*       Schedule Execution       *)
(* ****************************** *)

(* generate a schedule of length 500, run it, and assert
   that the predicate contained in `execute_and_check`
   holds at every step.
 *)
let sch = gen_schedule 500 in
let sim = SBAC_simulator.gen_simulator n_shards in
print_endline (string_of_bool (execute_and_check sim sch));;

(* ****************************** *)
(*       Example of Conflict      *)
(* (manually adding transactions) *)
(* ****************************** *)

let sim = SBAC_simulator.simulate_round sim;;              

let (t_0 : transaction)  = {
    tx_id          = 0;
    inputs         = [ ];
    outputs        = [ obj 0; obj 1; obj 2; obj 3 ];
  };;

(* t_1 and t_2 conflict on obj 0 *)
let (t_1 : transaction)  = {
    tx_id          = 1;
    inputs         = [ obj 0; obj 1; obj 3 ];
    outputs        = [ obj 4 ];
  };;

let (t_2 : transaction) = {
    tx_id            = 2;
    inputs           = [ obj 0; obj 2 ];
    outputs          = [ obj 5 ];
  }

(* populate some objects *)
let sim = SBAC_simulator.gen_simulator n_shards;;
let sim = SBAC_simulator.add_request sim (Initiate t_0);;

(* run till committed, around 10x *)
let sim = SBAC_simulator.simulate_round sim;;

(* add the conflicting objects *)
let sim = SBAC_simulator.add_request sim (Initiate t_1);;
let sim = SBAC_simulator.add_request sim (Initiate t_2);;

(* run until committed/aborted *)
let sim = SBAC_simulator.simulate_round sim;;
