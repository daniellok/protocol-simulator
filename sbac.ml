#use "pbft.ml";;

(* ****************************** *)
(*            Typedefs            *)
(* ****************************** *)

(* from the paper, the transaction should contain the procedure by
   which the output objects were obtained by the input objects so that
   smart contract checkers on-chain can determine the validity of the
   transactions. however, we do not really deal with smart contract
   checking here. *)
type transaction = {
    tx_id          : id;
 (* procedure      : obj list -> obj list   (* omitted *)             *)
    inputs         : obj list;              (* inputs and outputs are *)
    outputs        : obj list;              (* determined client-side *)
  };;

type status =
  | Committed
  | Aborted;;

type request =
  | Initiate of transaction;;
             
type internal =
  | Prepare        of transaction
  | Local_prepared of id * transaction * decision
  | Accept         of id * transaction * decision;;

type reply =
  | Reply of id * transaction * status;;

(* a shard is represented as a pair of a
   (2PC node, bunch of PBFT nodes)

   here, the global state is represented by a 2PC simulator
   (which contains a list of 2PC nodes), and a list of PBFT
   simulators (i.e. a 2D list of PBFT nodes). the two lists
   are of equal length, so the value stored in the `i`th 2PC
   node (in the left list) is decided on via PBFT consensus 
   in the `i`th PBFT simulator (in the right list).

   the simulator semantics keeps all timesteps in sync, and
   it also handles communication between the two lists. actual
   PBFT/2PC consensus is handled internally by their respective
   simulators.
 *)
type shard = {
    id         : int;                   (* id of the shard *)
    log        : internal list;         (* log for (local_)prepare messages *)
    pbft_nodes : PBFT_type._simulator;  (* the nodes store the values *)
  };;

let route (o : obj) : id =
  (o.id mod n_shards) + 1;;

let get_concerned_shards (t : transaction) : id list =
  let ins    = t.inputs in
  let outs   = t.outputs in
  let objs   = ins @ outs in
  let shards = List.map route objs in
  List.sort_uniq compare shards;;

let get_input_shards (t : transaction) : id list =
  let ins    = t.inputs in
  let shards = List.map route ins in
  List.sort_uniq compare shards;;

let get_output_shards (t : transaction) : id list =
  let outs   = t.outputs in
  let shards = List.map route outs in
  List.sort_uniq compare shards;;

(* ****************************** *)
(*     Initiate atomic commit     *)
(* ****************************** *)

(* When a shard receives a `prepare(T)` message, it tries to
   reach a decision about whether or not to commit the transaction T. 
   It reaches a decision via internal PBFT consensus.
 *)
let req_decide_consume (t : transaction) (s : shard) : shard =
  let pbft    = s.pbft_nodes in
  let objs    = List.filter (fun o -> route o = s.id) t.inputs in
  let req     = Request (t.tx_id, Decide (Consume objs)) in
  let pbft'   = PBFT_simulator.add_request pbft req in
  { s with pbft_nodes = pbft' };;

(* Because of the new request/reply segregation, the shard does not actually
   sequence the `prepare(T)` message. This might not be ideal, but practically
   speaking, we never check the log for the presence of a prepare anyway, and
   validity-checking will not involve checking the logs in general.
 *)
let deliver_initialize (r : request) (s : shard) :
      (internal option * reply option * shard) =
  match r with
  | Initiate t ->
     let concerned_shards = get_concerned_shards t in
     if List.mem s.id concerned_shards
     then (let s'  = req_decide_consume t s in
           let s'' = { s' with log = Prepare t :: s'.log } in
           (None, None, s''))
     else (None, None, s);;

(* ****************************** *)
(*         Commit / Abort         *)
(* ****************************** *)

(* When a shard receives a `local_prepared(id, T, decision)` message 
   from another shard, it logs it. if `decision` is `abort` in any of the
   messages, then all shards will move to abort. We assume an `honest shards`
   threat model here, so we do not consider the case that a shard may send
   different messages to different nodes.
 *)

(* checks a shard's log for `local_prepared` messages containing the
   transaction in question. there are three cases:

   1. we have received `commit` from all concerned nodes
      -> Some Commit
   2. we have received at least one `abort` from a concerned node
      -> Some Abort
   3. we have not heard back from all concerned nodes
      -> None
 *)
let check_log (t_init : transaction) (s : shard) : decision option =
  let concerned_shards = get_concerned_shards t_init in
  let rec traverse log responded decision : decision option =
    match log with
    | [] ->
       (* see if we've received responses from all concerned nodes *)
       let r_sorted  = List.sort_uniq compare responded in
       let cn_sorted = List.sort_uniq compare concerned_shards in
       if r_sorted = cn_sorted
       then Some decision  (* if so, commit *)
       else None           (* if not, we can't reach a decision *)
    | m :: log' ->
       (match m with
        | Local_prepared (id, t, d) ->
           if t = t_init && List.mem id concerned_shards
           then (match d with
                 | Commit -> traverse log' (id :: responded) decision
                 | Abort  -> traverse log' (id :: responded) Abort)
           else traverse log' responded decision
        | _ -> traverse log' responded decision)
  in
  traverse s.log [] Commit;;
  
let deliver_local_prepared (m : internal) (s : shard) :
      (internal option * reply option * shard) =
  match m with
  | Local_prepared (_, t, _) ->
     let concerned_shards = get_concerned_shards t in
     if List.mem s.id concerned_shards
     then (let s'     = { s with log = m :: s.log } in
           let result = check_log t s' in
           (match result with
            | Some d ->
               (let res = Accept (s.id, t, d) in
                (Some res, None, s'))
            | None ->
               (* no decision, just append to log *)
               (None, None, s')))
     else (None, None, s)
  | _ -> raise (Wrong_message_type "SBAC.deliver_local_prepared");;

(* ****************************** *)
(*             Accept             *)
(* ****************************** *)
let req_execute_consume (t : transaction) (s : shard) : shard =
  let pbft    = s.pbft_nodes in
  let objs    = List.filter (fun o -> route o = s.id) t.inputs in
  let req     = Request (t.tx_id, Execute (Consume objs)) in
  let pbft'   = PBFT_simulator.add_request pbft req in
  { s with pbft_nodes = pbft' };;

let req_execute_create (t : transaction) (s : shard) : shard =
  let pbft    = s.pbft_nodes in
  let objs    = List.filter (fun o -> route o = s.id) t.outputs in
  let req     = Request (t.tx_id, Execute (Create objs)) in
  let pbft'   = PBFT_simulator.add_request pbft req in
  { s with pbft_nodes = pbft' };;

let req_execute_release (t : transaction) (s : shard) : shard =
  let pbft    = s.pbft_nodes in
  let objs    = List.filter (fun o -> route o = s.id) t.inputs in
  let req     = Request (t.tx_id, Execute (Release objs)) in
  let pbft'   = PBFT_simulator.add_request pbft req in
  { s with pbft_nodes = pbft' };;

let deliver_accept (m : internal) (s : shard) :
      (internal option * reply option * shard) =
  match m with
  | Accept (sid, t, decision) ->
     let concerned_shards = get_concerned_shards t in
     (* a shard should only sequence its own accept *)
     if List.mem s.id concerned_shards && s.id = sid
     then (let s' = { s with log = m :: s.log } in
           match decision with
           | Commit ->
              (* execute transaction `t` via PBFT *)
              let s'' = (if List.mem s'.id (List.map route t.inputs)
                        then req_execute_consume t s'
                        else s') in
              let s''' = (if List.mem s''.id (List.map route t.outputs)
                         then req_execute_create t s''
                         else s'') in
              (None, None, s''')
           | Abort ->
              (* notify the client that the transaction has been aborted *)
              let s'' = req_execute_release t s' in
              (None, None, s''))
     else (None, None, s)
  | _ ->
     raise (Wrong_message_type "SBAC.deliver_accept");;

let deliver_message (m : internal) (s : shard) :
      (internal option * reply option * shard) =
  match m with
  | Prepare _ ->
     (* should never happen *)
     (None, None, s)
  | Local_prepared _ ->
     deliver_local_prepared m s
  | Accept _ ->
     deliver_accept m s;;

(* ****************************** *)
(*            Simulator           *)
(* ****************************** *)
  
(* we need to know when the internal PBFT nodes in a shard
   have come to a consensus. we do this by inspecting the 
   PBFT simulators' `client replies` field to see whether
   enough `Reply` messages have arrived for the ongoing
   transaction
 *)
let count_replies (r_init : pbft_reply) (rs_init : pbft_reply list) : int =
  let rec traverse tx_id status (rs : pbft_reply list) count =
    match rs with
    | [] ->
       count
    | r :: rs' ->
       (match r with
        | Reply (_, tx_id', status') ->
           if tx_id' = tx_id && status' = status
           then traverse tx_id status rs' (count + 1)
           else traverse tx_id status rs' count)
  in
  match r_init with
  | Reply (_, tx_id, status) ->
     traverse tx_id status rs_init 0;;

let count_replies_sbac (r_init : reply) (rs_init : reply list) : int =
  let rec traverse t status (rs : reply list) count =
    match rs with
    | [] ->
       count
    | r :: rs' ->
       (match r with
        | Reply (_, t', status') ->
           if t' = t && status' = status
           then traverse t status rs' (count + 1)
           else traverse t status rs' count)
  in
  match r_init with
  | Reply (_, t, status) ->
     traverse t status rs_init 0;;

let get_original_transaction (s : shard) (tx_id : id) : transaction =
  let rec traverse (log : internal list) : transaction =
    match log with
    | [] ->
       raise (Transaction_not_found)
    | m :: log' ->
       (match m with
        | Prepare t -> if t.tx_id = tx_id
                       then t
                       else traverse log'
        | _         -> traverse log')
  in
  traverse s.log;;

let convert_reply (s : shard) (r : pbft_reply) :
      (internal option * reply option) = 
  match r with
  | Reply (_, tx_id, status) ->
     let t = get_original_transaction s tx_id in
     (match status with
      (* 1. we've decided to commit/abort a transaction *)
      | Decision d ->
         let i = Local_prepared (s.id, t, d) in
         (Some i, None)
      (* 2. we've finished executing a transaction *)
      | New_values _ ->
         let rep = Reply (s.id, t, Committed) in
         (None, Some rep)
      (* 3. we've released all locks held by a transaction *)
      | New_locks _ ->
         let rep = Reply (s.id, t, Aborted) in
         (None, Some rep));;

let remove_tx_from_replies (crs_init : pbft_reply list) (tx_id : id) (s : pbft_status) =
  let rec traverse (crs : pbft_reply list) =
    match crs with
    | [] ->
       []
    | r :: crs' ->
       (match r with
        | Reply (_, tx_id', s') ->
           if tx_id' = tx_id && s' = s
           then traverse crs'
           else r :: traverse crs')
  in traverse crs_init;;

let remove_tx_from_replies_sbac (crs_init : reply list) (tx : transaction) =
  let rec traverse (crs : reply list) =
    match crs with
    | [] ->
       []
    | r :: crs' ->
       (match r with
        | Reply (_, tx', _) ->
           if tx' = tx
           then traverse crs'
           else r :: traverse crs')
  in traverse crs_init;;

(* this is where most of the work is done in the SBAC simulator.
   we need to do a few things at every round:
   
   1. advance each PBFT simulator 1 round
   2. check for any `client_responses` in the PBFT simulators
   3. handle the responses (if any) properly
 *)
let advance_shard (s : shard) : (internal option * reply option * shard) =
  let pbft_sim  = s.pbft_nodes in
  let pbft_sim' = PBFT_simulator.simulate_round pbft_sim in
  let replies = pbft_sim'.client_replies in
  let rec traverse (rs : pbft_reply list) : pbft_reply option =
    match rs with
    | [] ->
       None
    | r :: rs' ->
       if count_replies r replies >= 2 * f + 1
       then Some r
       else traverse rs'
  in
  (* check client_replies to see if any one has 2f + 1 occurences *)
  let r_opt = traverse replies in
  match (r_opt) with
  | None ->
     (* if not, move on *)
     (let s' = { s with pbft_nodes = pbft_sim' } in
      (None, None, s'))
  | Some r ->
     (* if so, we need to convert the PBFT client responses to an
        SBAC message. *)
     (match r with
      | Reply (_, tx_id, status) ->
         let replies'         = remove_tx_from_replies
                                  pbft_sim'.client_replies
                                  tx_id status in
         let (handled : pbft_reply) = Reply (-1, tx_id, status) in
         let pbft_sim''       = { pbft_sim' with client_replies  = replies';
                                                 handled_replies =
                                                   handled :: pbft_sim'.handled_replies } in
         let s'               = { s with pbft_nodes = pbft_sim'' } in
         let (int_opt, r_opt) = convert_reply s' r in
         (int_opt, r_opt, s'));;

let gen_nodes (n : int) : shard list =
  let rec accumulate (n : int) (acc : shard list) =
    if n = 0
    then acc
    else accumulate (n - 1)
           ({ id = n;
              log = [];
              pbft_nodes = PBFT_simulator.gen_simulator (3 * f + 1);
            } :: acc)
  in
  accumulate n [];;

module SBAC_type : (Sim_type
                    with type _node     = shard
                     and type _request  = request
                     and type _internal = internal
                     and type _reply    = reply) =
  struct
    type _node      = shard
    type _request   = request
    type _internal  = internal
    type _reply     = reply
    type _simulator = {
        nodes              : shard    list;
        message_queue      : internal list;
        next_message_queue : internal list;
        client_replies     : reply    list;
        handled_replies    : reply    list;
        client_requests    : request  list;
        handled_requests   : request  list;
        timestep           : int;
      }

    let deliver_request  = deliver_initialize
    let deliver_internal = deliver_message

    (* Once a transaction is done, move the reply from `client_replies`
       to `handled_replies`.
     *)
    let rec filter_commits (s : _simulator) : _simulator =
      let rec traverse replies : (transaction * status) option =
        match replies with
        | [] ->
           None
        | r :: replies' ->
           (match r with
            | Reply (_, t, status) ->
               if (count_replies_sbac r s.client_replies
                   = List.length (get_concerned_shards t))
               then Some (t, status)
               else traverse replies')
      in
      let ts_opt = traverse s.client_replies in
      match ts_opt with
      | Some (t, status) ->
         let r  = Reply (-1, t, status) in
         let s' = { s with client_replies  = remove_tx_from_replies_sbac
                                              s.client_replies t;
                           handled_replies = r :: s.handled_replies } in
         filter_commits s'
      | None ->
         s;;

    let extra_round_actions (s : _simulator) : _simulator =
      let rec traverse (shards : shard list) (nmq : internal option list)
                (reps : reply option list) (acc : shard list) :
                (internal option list * reply option list * shard list) =
        match shards with
        | [] ->
           (nmq, reps, List.rev acc)
        | shard :: shards' ->
           let (int_opt, rep_opt, shard') = advance_shard shard in
           traverse shards' (int_opt :: nmq) (rep_opt :: reps) (shard' :: acc)
      in
      let (int_opts, rep_opts, shards) = traverse s.nodes [] [] [] in
      let internals = remove_options int_opts in
      let replies   = remove_options rep_opts in
      let s' = { s with nodes          = shards;
                        message_queue  = s.message_queue  @ internals;
                        client_replies = s.client_replies @ replies } in
      filter_commits s'

     let gen_nodes = gen_nodes
  end;;

module SBAC_simulator = Sim (SBAC_type);;

(* ****************************** *)
(*           Scheduling           *)
(* ****************************** *)

let tx_id  = ref 0;;
let obj_id = ref 1;;

type instruction =
  | Add_transaction of transaction;;

type schedule = (instruction list) list;;

let gen_output_obj () : obj =
  let i = !obj_id in
  let o = obj i in
  obj_id := !obj_id + 1;
  o;;

let gen_input_obj () : obj =
  let r = Random.int !obj_id in
  obj r;;

let gen_outputs chance : obj list =
  if chance = 0
  then []
  else (let rec add items acc =
          if items = 0
          then acc
          else (let roll = Random.int chance in
                if roll = 0
                then (let o = gen_output_obj () in
                      add (items - 1) (o :: acc))
                else add (items - 1) acc)
        in
        add max_obj_out []);;

let gen_inputs chance : obj list =
  if chance = 0
  then []
  else (let rec add items acc =
          if items = 0
          then acc
          else (let roll = Random.int chance in
                if roll = 0
                then (let o = gen_input_obj () in
                      add (items - 1) (o :: acc))
                else add (items - 1) acc)
        in
        add max_obj_in []);;

let gen_tx in_chance out_chance : transaction =
  let i    = !tx_id in
  let ins  = gen_inputs in_chance in
  let outs = gen_outputs out_chance in
  tx_id := !tx_id + 1;
  {
    tx_id          = i;
    inputs         = ins;
    outputs        = outs;
  };;

let gen_schedule_element max_tx in_chance out_chance : instruction list =
  let rec add items element =
    if items = 0
    then element
    else (let roll = Random.int add_chance in
          if roll = 0
          then (let t = gen_tx in_chance out_chance in
                add (items - 1) (Add_transaction t :: element))
          else add (items - 1) element)
  in
  let element  = add max_tx [] in
  element;;

let gen_schedule (rounds_init : int) : schedule =
  let rec add rounds =
    if rounds = 0
    then []
    else (if rounds > (rounds_init - 5)
          then gen_schedule_element max_transactions 0 1 :: add (rounds - 1)
          else (if rounds > (rounds_init - 15)
                then gen_schedule_element 0 0 0 :: add (rounds - 1)
                else (if rounds < 11
                      then [] :: add (rounds - 1)
                      else gen_schedule_element max_transactions
                             obj_in_chance obj_out_chance :: add (rounds - 1))))
  in
  add rounds_init;;

let execute_instruction (s : SBAC_type._simulator) (i : instruction) =
  match i with
  | Add_transaction t ->
     let req = Initiate t in
     SBAC_simulator.add_request s req;;
     
let execute_schedule_element (s_init : SBAC_type._simulator) (instrs_init : instruction list) =
  let rec exec s instrs =
    match instrs with
    | [] ->
       s
    | instr :: instrs' ->
       exec (execute_instruction s instr) instrs'
  in
  exec s_init instrs_init;;

let execute_schedule (s_init : SBAC_type._simulator) (sch_init : schedule) =
  let rec exec s sch =
    match sch with
    | [] ->
       s
    | instrs :: sch' ->
       let s' = (execute_schedule_element s instrs) in
       exec (SBAC_simulator.simulate_round s') sch'
  in
  exec s_init sch_init;;

(* ****************************** *)
(*       Predicate Checking       *)
(* ****************************** *)

(* Check the list of handled_responses. At every stage of any schedule's 
   execution, there should be no committed transactions with any input
   objects in common.
 *)

(* helper function, used to check the uniqueness of the input objects in
   transactions. returns None if an input object has already been seen. 
   else, return an extended list of seen objects.
 *)
let rec add_to_objects (inputs : obj list) (seen : obj list) : (obj list) option =
  match inputs with
  | [] ->
     Some seen
  | o :: inputs' ->
     if List.mem o seen
     then None
     else add_to_objects inputs' (o :: seen);;

let pred_no_conflict (sim : SBAC_type._simulator) : bool =
  let rec traverse (reps : reply list) (objs : obj list) : bool =
    match reps with
    | [] ->
       true
    | r :: reps' ->
       (match r with
        | Reply (_, tx, status) ->
           if status = Committed
           then (let objs'_opt = add_to_objects tx.inputs objs in
                 match objs'_opt with
                 | Some objs' ->
                    traverse reps' objs'
                 | None ->
                    false)
           else traverse reps' objs)
  in
  traverse sim.handled_replies [];;

(* execute the schedule and check the trace at every step *)
let execute_and_check (sim_init : SBAC_type._simulator) (sch_init : schedule) : bool =
  let rec traverse sim sch =
    match sch with
    | [] ->
       pred_no_conflict sim
    | instrs :: sch' ->
       let sim' = execute_schedule_element sim instrs in
       pred_no_conflict sim' && traverse sim' sch'
  in
  traverse sim_init sch_init;;
