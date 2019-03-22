#use "config.ml";;
#use "sim.ml";;

(* ****************************** *)
(*            Typedefs            *)
(* ****************************** *)
type pbft_procedure =
  | Consume of obj list
  | Create  of obj list
  | Release of obj list;;

type operation =
  | Decide  of pbft_procedure
  | Execute of pbft_procedure;;

type lock = {
    obj_id : int;
    tx_id  : int;
  };;

let make_lock obj_id tx_id =
  { obj_id = obj_id; tx_id = tx_id };;

type pbft_status =
  | Decision   of decision
  | New_values of obj list
  | New_locks  of lock list;;

type pbft_request =
  | Request of (id * operation);;  (* tx_id * operation *)

type pbft_reply =
  | Reply of (id * id * pbft_status);;  (* replica id * tx_id * status *)

type pbft_internal =
  | Pre_prepare of (int * pbft_request)         (* seq_num * request *)
  | Prepare     of (int * pbft_request * id)    (* seq_num * request * replica id *)
  | Commit      of (int * pbft_request * id);;  (* seq_num * request * replica id *)

type replica = {
    id        : int;
    objects   : obj list;
    locks     : lock list;
    log       : pbft_internal list;
    seq_num   : int;
    last_exec : int;
    primary   : bool;
    is_faulty : bool;
  };;

(* ****************************** *)
(*         Misc. Utilities        *)
(* ****************************** *)
let append_to_log (m : pbft_internal) (r : replica) =
  { r with log = m :: r.log };;

let increase_seq_num (r : replica) =
  { r with seq_num = r.seq_num + 1 };;

(* assert message equality, ignore replica numbers for prepare and commit *)
let match_message (m : pbft_internal) (m' : pbft_internal) : bool =
  match m with
  | Prepare (n, r, _) ->
     (match m' with
      | Prepare (n', r', _) ->
         (n = n' && r = r')
      | _ ->
         false)
  | Commit (n, r, _) ->
     (match m' with
      | Commit (n', r', _) ->
         (n = n' && r = r')
      | _ ->
         false)
  | _ ->
     m = m';;

(* counts the number of messages in a log (ignoring replica numbers)
   POTENTIAL BUG: this relies on the property that duplicate messages do not
                  get sequenced.
 *)
let count_message (m_init : pbft_internal) (log_init : pbft_internal list) : int =
  let rec traverse m log n =
    match log with
    | [] ->
       n
    | m' :: log' ->
       if (match_message m m')
       then traverse m log' (n + 1)
       else traverse m log' n
  in traverse m_init log_init 0;;

(* same as above, but do not ignore replica numbers *)
let count_message_strict (m_init : pbft_internal) (log_init : pbft_internal list) : int =
  let rec traverse m log n =
    match log with
    | [] ->
       n
    | m' :: log' ->
       if m' = m
       then traverse m log' (n + 1)
       else traverse m log' n
  in traverse m_init log_init 0;;

(* ****************************** *)
(*          PHASE 1 & 2           *)
(* ****************************** *)

(* Deliver the client's request to the primary *)
let deliver_request (req : pbft_request) (r : replica) :
      (pbft_internal option * replica) =
  if r.primary
  then (let n = r.seq_num in
        let response = Pre_prepare (n, req) in
        let r' = increase_seq_num r in
        (Some response, r'))
  else (None, r);;

(* ****************************** *)
(*             PHASE 3            *)
(* ****************************** *)
(* delivers the pre-prepare, multicasting a prepare if accepted *)
let deliver_pre_prepare (m : pbft_internal) (r : replica) :
      (pbft_internal option * pbft_reply option * replica) =
  match m with
  | Pre_prepare (n, req) ->
     if (List.mem m r.log)
     then (None, None, r)
     else (let response = Prepare (n, req, r.id) in
            let r' = append_to_log m r in
            (Some response, None, r'))
  | _ -> raise (Wrong_message_type "deliver_pre_prepare");;

(* ****************************** *)
(*          PHASE 4 & 5           *)
(* ****************************** *)

(* the predicate prepared(r, n, i), but `i` (the index of a replica) 
   is replaced by the replica itself. `f` comes from config.ml. *)
let pred_prepared (req : pbft_request) (n : int) (r : replica) : bool =
  (* check that log has a pre-prepare for `n` `r` *)
  (let pre_prepare = Pre_prepare (n, req)
   in List.mem pre_prepare r.log)
  (* and check that log has 2f prepares *)
  && (let prepare = Prepare (n, req, -1)
      in (count_message prepare r.log) >= 2 * f + 1);;

(* delivers a prepare, and checks the predicate `prepared` after delivery.
   if true, multicast a commit. *)
let deliver_prepare (m : pbft_internal) (r : replica) :
      (pbft_internal option * pbft_reply option * replica) =
  match m with
  | Prepare (n, req, _) ->
     if List.mem m r.log
     then (None, None, r)
     else (let r' = append_to_log m r
           in if pred_prepared req n r'
              then (let response = Commit (n, req, r.id) in
                    (Some response, None, r'))
              else (None, None, r'))
  | _ -> raise (Wrong_message_type "deliver_prepare");;

(* ****************************** *)
(*             PHASE 6            *)
(* ****************************** *)
let append_to_locks (locks1 : lock list) (locks2: lock list) : lock list =
  let rec traverse locks acc =
    match locks with
    | [] ->
       acc
    | lock :: locks' ->
       if List.mem lock acc
       then traverse locks' acc
       else traverse locks' (lock :: acc)
  in
  traverse locks1 locks2;;

let check_locks (locks_init : lock list) (obj_id : id) : bool =
  let rec traverse locks =
    match locks with
    | [] ->
       false
    | lock :: locks' ->
       lock.obj_id = obj_id || traverse locks'
  in
  traverse locks_init;;

let lock (tx_id : int) (objs : obj list) (r : replica) : replica =
  let locks     = List.map (fun (o : obj) -> make_lock o.id tx_id) objs in
  let all_locks = append_to_locks locks r.locks in
  { r with locks = all_locks };;

let release (tx_id : int) (objs : obj list) (r : replica) : replica =
  let locks    = List.map (fun (o : obj) -> make_lock o.id tx_id) objs in
  let filtered = List.filter (fun l -> not (List.mem l locks)) r.locks in
  { r with locks = filtered };;

let decide_procedure (tx_id : int) (p : pbft_procedure) (r : replica) :
      (decision * replica) =
  match p with
  | Consume objs ->
     if (List.for_all (fun o -> List.mem o r.objects) objs
         && List.for_all (fun (o : obj) -> not (check_locks r.locks o.id)) objs)
     then (let r' = lock tx_id objs r in
           (Commit, r'))
     else (Abort, r)
  | Create objs ->
     (Commit, r)
  | Release objs ->
     (Commit, r);;

let execute_procedure (tx_id : int) (p : pbft_procedure) (r : replica) : replica =
  let r' = { r with last_exec = r.last_exec + 1 } in
  match p with
  | Consume objs ->
     let r'' = release tx_id objs r' in
     { r'' with objects = List.filter (fun a -> not (List.mem a objs)) r'.objects }
  | Create objs ->
     { r' with objects = objs @ r'.objects }
  | Release objs ->
     release tx_id objs r';;

(* perform request *)
let execute_request (req : pbft_request) (r : replica) :
      (pbft_internal option * pbft_reply option * replica)  =
  match req with
  | Request (i, o) ->
     (match o with
      | Execute p ->
         let r' = execute_procedure i p r in
         (match p with
          | Release _ ->
             let reply = Reply (r'.id, i, New_locks r'.locks) in
             (None, Some reply, r')
          | _ ->
             let reply = Reply (r'.id, i, New_values r'.objects) in
             (None, Some reply, r'))
      | Decide p ->
         let decision, r' = decide_procedure i p r in
         let r''          = { r' with last_exec = r'.last_exec + 1 } in
         let reply        = Reply (r''.id, i, Decision decision) in
         (None, Some reply, r''));;

(* the predicate committed-local *)
let pred_committed_local (req : pbft_request) (n : int) (r : replica) : bool =
  let log = r.log in
  (pred_prepared req n r)
  && (let commit = Commit (n, req, -1)
      in (count_message commit log >= 2 * f + 1));;

(* deliver a commit message and check if committed-local is true.
   also check if the replica has already executed all operations with
   a lower sequence number. if so, then execute the request referred 
   to by `d`, and issue a reply back to the client. *)
let deliver_commit (m : pbft_internal) (r : replica) :
      (pbft_internal option * pbft_reply option * replica) =
  match m with
  | Commit (n, req, _) ->
     if List.mem m r.log
     then (None, None, r)
     else (let r' = append_to_log m r in
           if (pred_committed_local req n r'
               && n = r'.last_exec + 1)
           then execute_request req r'
           else (None, None, r'))
  | _ -> raise (Wrong_message_type "deliver_commit");;

(* ****************************** *)
(*       Byzantine Versions       *)
(* ****************************** *)

(* We assume that all non-faulty nodes know who the leader is (i.e. their
   public key). Hence, a faulty node cannot spoof the leader's signature,
   and therefore cannot send out pre-prepares since only the leader is allowed
   to do so.
 *)

let gen_random_obj_list () =
  match Random.int 4 with
  | 0 -> []
  | 1 -> [ obj (Random.int 100) ]
  | 2 -> [ obj (Random.int 100); obj (Random.int 100) ]
  | _ -> [ obj (Random.int 100); obj (Random.int 100); obj (Random.int 100) ];;
    
let gen_random_proc () =
  match Random.int 3 with
  | 0 -> Consume (gen_random_obj_list ())
  | 1 -> Create (gen_random_obj_list ())
  | _ -> Release (gen_random_obj_list ());;

let gen_random_op () =
  match Random.int 2 with
  | 0 -> Decide (gen_random_proc ())
  | _ -> Execute (gen_random_proc ());;

let gen_random_req id =
  Request (id, gen_random_op ());;

let gen_random_decision () : decision =
  match Random.int 2 with
  | 0 -> Commit
  | _ -> Abort;;

let gen_random_lock_list () =
  match Random.int 4 with
  | 0 -> []
  | 1 -> [ make_lock (Random.int 100) (Random.int 100) ]
  | 2 -> [ make_lock (Random.int 100) (Random.int 100);
           make_lock (Random.int 100) (Random.int 100); ]
  | _ -> [ make_lock (Random.int 100) (Random.int 100);
           make_lock (Random.int 100) (Random.int 100);
           make_lock (Random.int 100) (Random.int 100); ];;

let gen_random_status () =
  match Random.int 3 with
  | 0 -> Decision (gen_random_decision ())
  | 1 -> New_values (gen_random_obj_list ())
  | _ -> New_locks (gen_random_lock_list ());;

let gen_random_response (n : int) (tx_id : id) (r : replica) =
  let roll1 = Random.int 3 in
  let roll2 = Random.int 2 in
  (let int_opt =
     match roll1 with
     | 0 -> Some (Prepare (n, gen_random_req tx_id, r.id))
     | 1 -> Some (Commit  (n, gen_random_req tx_id, r.id))
     | _ -> None
   in
   let rep_opt =
     match roll2 with
     | 0 -> Some (Reply (r.id, tx_id, gen_random_status ()))
     | _ -> None
   in
   (int_opt, rep_opt, r))

let get_tx_id (req : pbft_request) =
  match req with
  | Request (tx_id, _) -> tx_id;;
  
let deliver_internal_random (m : pbft_internal) (r : replica) :
      (pbft_internal option * pbft_reply option * replica) =
  match m with
  | Pre_prepare (n, req) ->
     gen_random_response n (get_tx_id req) r
  | Prepare (n, req, _) ->
     gen_random_response n (get_tx_id req) r
  | Commit (n, req, _) ->
     gen_random_response n (get_tx_id req) r;;

(* ****************************** *)
(*            Delivery            *)
(* ****************************** *)

(* Deliver a message to a replica r, and receive a response
   and the replica's updated state. Replicas will be stored
   in a list in a global simulator. *)
let deliver_internal (m : pbft_internal) (r : replica) :
      (pbft_internal option * pbft_reply option * replica) =
  if List.mem m r.log
  then (None, None, r)
  else (if r.is_faulty
        then deliver_internal_random m r
        else (match m with
              | Pre_prepare _ ->
                 deliver_pre_prepare m r
              | Prepare _ ->
                 deliver_prepare m r
              | Commit _ ->
                 deliver_commit m r));;

(* ****************************** *)
(*            Simulator           *)
(* ****************************** *)

(* function to generate `n` replicas with sequential ids *)
let gen_replicas (n_init : int) : replica list =
  let rec gen (n : int) (replicas : replica list) : replica list =
    if n = 0
    then replicas
    else (let (rep : replica) = {
              id        = n;
              objects   = [];
              locks     = [];
              log       = [];
              seq_num   = 1;
              last_exec = 0;
              primary   = false;
              is_faulty = false;
            } in
          let rep_primary = { rep with primary   = true } in
          let rep_faulty  = { rep with is_faulty = true } in
          if n = 1
          then gen (n - 1) (rep_primary :: replicas)
          else (if n > (n_init - f)
                then gen (n - 1) (rep_faulty :: replicas)
                else gen (n - 1) (rep :: replicas)))
  in gen n_init [];;

(* A request can be committed but not executed if its sequence number
   is higher than the last executed operation. This is to enforce a total
   order on the operations. Therefore, at each round we need to make sure
   we haven't forgotten to execute them, otherwise they will just be
   pending forever. *)

(* check for commits with n = r.last_exec + 1, and with 2f + 1 instances *)
let check_pending_commit (n : int) (log_init : pbft_internal list) :
      pbft_request option =
  let rec traverse log =
    match log with
    | [] ->
       None
    | m :: log' ->
       (match m with
        | Commit (n', req, _) ->
           if n' = n
           then (if count_message m log_init >= (2 * f + 1)
                 then (Some req)
                 else traverse log')
           else traverse log'
        | _ ->
           traverse log')
  in traverse log_init;;
  
let execute_pending_commits (r : replica) :
      (pbft_internal option * pbft_reply option * replica) =
  let req_opt = check_pending_commit (r.last_exec + 1) r.log in
  match req_opt with
  | None ->
     (None, None, r)
  | Some m ->
     execute_request m r;;

module PBFT_type : (Sim_type
                    with type _node     = replica
                     and type _request  = pbft_request
                     and type _internal = pbft_internal
                     and type _reply    = pbft_reply) =
  struct
    type _node      = replica
    type _request   = pbft_request
    type _internal  = pbft_internal
    type _reply     = pbft_reply
    type _simulator = {
        nodes              : replica       list;
        message_queue      : pbft_internal list;
        next_message_queue : pbft_internal list;
        client_replies     : pbft_reply    list;
        handled_replies    : pbft_reply    list;
        client_requests    : pbft_request  list;
        handled_requests   : pbft_request  list;
        timestep           : int;
      }
    let deliver_request  = deliver_request
    let deliver_internal = deliver_internal
                        
    let extra_round_actions (s : _simulator) =
      let rec traverse (rs : _node list) (next_rs : _node list) (reps : _reply list) : _node list * _reply list =
        match rs with
        | [] ->
           (next_rs, reps)
        | r :: rs' ->
           (let (_, rep_opt, r') = execute_pending_commits r in
            match rep_opt with
            | None     -> traverse rs' (r' :: next_rs) reps
            | Some rep -> traverse rs' (r' :: next_rs) (rep :: reps))
      in
      let (next_rs, reps) = traverse s.nodes [] s.client_replies in
      let s' = { s with nodes          = next_rs;
                        client_replies = reps; } in
      s';;
      
    let gen_nodes = gen_replicas
  end;;

module PBFT_simulator = Sim (PBFT_type);;

let sim = PBFT_simulator.gen_simulator (3 * f + 1);;
let req = Request (0, Execute (Create [obj 1; obj 2; obj 3]));;
let sim = PBFT_simulator.add_request sim req;;
let sim = PBFT_simulator.simulate_round sim;;
