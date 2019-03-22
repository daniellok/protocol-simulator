#use "common.ml";;

module type Sim_type =
  sig
    type _node
    type _request
    type _internal
    type _reply
    type _simulator = {
        nodes              : _node     list;
        message_queue      : _internal list;
        next_message_queue : _internal list;
        client_replies     : _reply    list;
        handled_replies    : _reply    list; (* to persist replies *)
        client_requests    : _request  list;
        handled_requests   : _request  list; (* to persist requests *)
        timestep           : int;
      }
    val deliver_request     : _request -> _node -> (_internal option * _node)
    val deliver_internal    : _internal -> _node -> (_internal option
                                                    * _reply option
                                                    * _node)
    val extra_round_actions : _simulator -> _simulator
    val gen_nodes           : int -> _node list
  end;;

module Sim (S : Sim_type) =
  struct
    (* simulates delivering a request to all nodes *)
    let multicast_req (r : S._request) (sim : S._simulator) :
          (S._internal list * S._node list) =
      let rec traverse (nodes : S._node list)
                (acc : S._internal option list * S._node list) =
        match nodes with
        | [] ->
           acc
        | n :: nodes' ->
           let int_opt, n'  = S.deliver_request r n           in
           let int_opts, ns = acc                             in
           let acc'         = (int_opt :: int_opts, n' :: ns) in
           traverse nodes' acc'
      in
      let int_opts, nodes' = traverse sim.nodes ([], []) in
      let internals        = remove_options int_opts     in
      (internals, List.rev nodes')

    let rec deliver_requests (sim : S._simulator) : S._simulator =
      match sim.client_requests with
      | [] ->
         sim
      | r :: requests' ->
         let internals, nodes = multicast_req r sim in
         let sim' = {
             sim with nodes            = nodes;
                      message_queue    = sim.message_queue @ internals;
                      client_requests  = requests';
                      handled_requests = r :: sim.handled_requests;
           } in
         deliver_requests sim'

    (* simulates multicasting an internal message to all nodes *)
    let multicast_int (m : S._internal) (sim : S._simulator) :
          (S._internal list * S._reply list * S._node list) =
      let rec traverse (nodes : S._node list)
                (acc : S._internal option list
                       * S._reply option list
                       * S._node list) :
                (S._internal option list
                 * S._reply option list
                 * S._node list) =
        match nodes with
        | [] ->
           acc
        | n :: nodes' ->
           let int_opt, rep_opt, n'   = S.deliver_internal m n in
           let int_opts, rep_opts, ns = acc in
           let acc' = (int_opt :: int_opts, rep_opt :: rep_opts, n' :: ns) in
           traverse nodes' acc'
      in
      let int_opts, rep_opts, nodes' = traverse sim.nodes ([], [], []) in
      let internals = remove_options int_opts in
      let replies   = remove_options rep_opts in
      (internals, replies, List.rev nodes')

    (* multicasts all the messages in the message queue,
       retrieves replies, and adds them to the next message queue. *)
    let rec deliver_messages (sim : S._simulator) : S._simulator =
      match sim.message_queue with
      | [] ->
         sim
      | msg :: message_queue' ->
         let internals, replies, nodes = multicast_int msg sim in
         let sim' = {
             sim with nodes              = nodes;
                      message_queue      = message_queue';
                      next_message_queue = append_uniq
                                             internals
                                             sim.next_message_queue;
                      client_replies     = append_uniq
                                             replies
                                             sim.client_replies;
           } in
         deliver_messages sim'

    (* at the end of a round, swap in the next message queue,
       and set the new next message queue to empty. *)
    let swap_queues (sim : S._simulator) : S._simulator =
      { sim with message_queue = sim.next_message_queue;
                 next_message_queue = [] }

    (* increment the timestep by one. *)
    let increment_timestep (sim : S._simulator) : S._simulator =
      { sim with timestep = sim.timestep + 1 }

    (* simulate a single round of message passing. *)
    let simulate_round (sim : S._simulator) : S._simulator =
      let sim' = increment_timestep (
                     S.extra_round_actions (
                         swap_queues (
                             deliver_messages (
                                 deliver_requests sim
                   )))) in
      sim'

    (* generate an initial simulator *)
    let gen_simulator (n : int) : S._simulator = {
        nodes              = S.gen_nodes n;
        message_queue      = [];
        next_message_queue = [];
        client_requests    = [];
        handled_requests   = [];
        client_replies     = [];
        handled_replies    = [];
        timestep           = 0;
      }

    let add_request (sim : S._simulator) (m : S._request) =
      { sim with client_requests = m :: sim.client_requests }
  end;;
