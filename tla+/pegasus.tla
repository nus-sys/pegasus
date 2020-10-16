------------------------------- MODULE pegasus --------------------------------
(*

  Specifies the Pegasus protocol.

*)

EXTENDS Naturals, FiniteSets, Sequences, TLC

--------------------------------------------------------------------------------
(* `^\textbf{\large Constants and Variables}^' *)

CONSTANTS keys,   \* Set of model values representing keys
          servers \* Set of model values representing the servers

ASSUME /\ IsFiniteSet(keys)
       /\ IsFiniteSet(servers)

VARIABLES messages,    \* The network, a set of all messages sent
          switchState, \* The state of the Pegasus switch
          serverState, \* The states of the Pegasus servers
          requestID    \* Next unique write request ID

(*
  `^\textbf{Message Schemas}^'

  ClientWrite (Client to switch)
      [ mtype     |-> MClientWrite,
        requestID |-> i \in (1..),
        key       |-> k \in keys ]

  Write (Switch to servers)
      [ mtype      |-> MWrite,
        key        |-> k \in keys,
        requestID  |-> i \in (1..),
        ver        |-> i \in (1..),
        replicated |-> bool,
        dst        |-> s \in servers,
        replicas   |-> S \subseteq servers (empty if not replicated) ]

  Read (Switch to servers)
      [ mtype          |-> MRead,
        key            |-> k \in keys,
        dst            |-> s \in servers
        ghostLastReply |-> i \in (1..) ]

  WriteReply (Servers to switch)
      [ mtype     |-> MWriteReply,
        key       |-> k \in keys,
        sender    |-> s \in servers,
        requestID |-> i \in (1..),
        ver       |-> i \in (1..) ]

  ReadReply (Servers to switch)
      [ mtype          |-> MReadReply,
        key            |-> k \in keys,
        ver            |-> i \in (1..),
        ghostLastReply |-> i \in (1..) ]

  ClientWriteReply (Switch to clients)
      [ mtype     |-> MClientWriteReply,
        key       |-> k \in keys,
        requestID |-> i \in (1..),
        ghostVer  |-> i \in (1..) ]

  ClientReadReply (Switch to clients)
      [ mtype          |-> MClientReadReply,
        key            |-> k \in keys,
        ver            |-> i \in (1..),
        ghostLastReply |-> i \in (1..) ]
*)

CONSTANTS MClientWrite,
          MWrite,
          MRead,
          MWriteReply,
          MReadReply,
          MClientWriteReply,
          MClientReadReply

Init == /\ messages = {}
        /\ switchState = [ ver_next      |-> 1,
                           rkeys         |-> {},
                           rset          |-> [ k \in keys |-> {} ],
                           ver_completed |-> [ k \in keys |-> 0 ],
                           id_to_node    |-> [ s \in {} |-> {} ],
                           home_node     |-> [ s \in {} |-> {} ] ]
        /\ serverState = [ s \in servers |->
                           [ rkeys        |-> {},
                             kv_store     |-> [ k \in keys |-> 0 ],
                             client_table |-> {} ]]
        /\ requestID = 1

--------------------------------------------------------------------------------
(* `^\textbf{\large Helper and Utility Functions}^' *)

Max(S) == CHOOSE s \in S : \A sp \in S : sp <= s

\* Switch's stable mappings
HomeNode(k, s) == IF k \in DOMAIN switchState.home_node
                  THEN switchState.home_node
                  ELSE (k :> s) @@ switchState.home_node

IDToNode(id, s) == IF id \in DOMAIN switchState.id_to_node
                   THEN switchState.id_to_node
                   ELSE (id :> s) @@ switchState.id_to_node

\* Short-hand way of sending a message
Send(m) == messages' = messages \cup {m}

--------------------------------------------------------------------------------
(* `^\textbf{\large Main Spec}^' *)

clientWriteReplies == { m \in messages : m.mtype = MClientWriteReply }

NoDuplicateWrites ==
  \A m \in clientWriteReplies :
  Cardinality({ mp \in clientWriteReplies : mp.requestID = m.requestID}) = 1

ReadsLinearizable ==
  \A m \in { mp \in messages : mp.mtype = MClientReadReply } :
  m.ver >= m.ghostLastReply

Safety == NoDuplicateWrites /\ ReadsLinearizable

--------------------------------------------------------------------------------
(* `^\textbf{\large Actions and Message Handlers}^' *)

\* Client sends a write for key k
SendClientWrite(k) ==
  /\ requestID' = requestID + 1
  /\ Send([ mtype     |-> MClientWrite,
            requestID |-> requestID,
            key       |-> k ])
  /\ UNCHANGED << switchState, serverState >>

\* Switch handles ClientWrite w
HandleClientWrite(m) ==
LET
  isReplicated == m.key \in switchState.rkeys
IN
\E s \in servers : \E S \in SUBSET servers :
  /\ \/ /\ isReplicated
        /\ Send([ mtype     |-> MWrite,
                 key        |-> m.key,
                 requestID  |-> m.requestID,
                 ver        |-> switchState.ver_next,
                 replicated |-> TRUE,
                 dst        |-> IDToNode(m.requestID, s)[m.requestID],
                 replicas   |-> S ])
        /\ switchState' = [ switchState EXCEPT !.ver_next = @ + 1,
                                     !.id_to_node = IDToNode(m.requestID, s) ]
     \/ /\ ~isReplicated
        /\ Send([ mtype     |-> MWrite,
                 key        |-> m.key,
                 requestID  |-> m.requestID,
                 ver        |-> switchState.ver_next,
                 replicated |-> FALSE,
                 dst        |-> HomeNode(m.key, s)[m.key],
                 replicas   |-> {} ])
        /\ switchState' = [ switchState EXCEPT !.ver_next = @ + 1,
                                               !.home_node = HomeNode(m.key, s) ]
  /\ UNCHANGED << serverState, requestID >>

\* Switch issues a read for key k
SendRead(k) ==
LET
  isReplicated == k \in switchState.rkeys
  \* Find the last write version number that has been replied to
  lastReply == Max(
    { m.ghostVer : m \in { m \in messages : m.mtype = MClientWriteReply /\ m.key = k }} \cup
    { m.ver : m \in { m \in messages : m.mtype = MClientReadReply /\ m.key = k }} \cup
    { 0 }
  )
IN
  /\ \/ /\ isReplicated
        /\ \E s \in switchState.rset[k] :
           Send([ mtype          |-> MRead,
                  key            |-> k,
                  dst            |-> s,
                  ghostLastReply |-> lastReply ])
        /\ UNCHANGED switchState
     \/ /\ ~isReplicated
        /\ \E s \in servers :
           /\ Send([ mtype          |-> MRead,
                     key            |-> k,
                     dst            |-> HomeNode(k, s)[k],
                     ghostLastReply |-> lastReply ])
           /\ switchState' = [ switchState EXCEPT !.home_node = HomeNode(k, s) ]
  /\ UNCHANGED << serverState, requestID  >>

\* Destination server handles write m
HandleWrite(m) ==
LET
  s == serverState[m.dst]
  writeIsOld == s.kv_store[m.key] > m.ver
  newKVStore == IF writeIsOld THEN s.kv_store ELSE (m.key :> m.ver) @@ s.kv_store
  forwardedWrites == {[ m EXCEPT !.dst = f, !.replicas = {}] : f \in m.replicas}
IN
  \* Only process the write if the server knows it should be replicated
  /\ m.replicated = (m.key \in s.rkeys)
  \* Don't process if in the client table (equivalent to resending response)
  /\ m.requestID \notin s.client_table
  /\ messages' = messages \cup forwardedWrites \cup {
                 [ mtype     |-> MWriteReply,
                   key       |-> m.key,
                   sender    |-> m.dst,
                   requestID |-> m.requestID,
                   ver       |-> m.ver ]}
  /\ serverState' = [ serverState EXCEPT ![m.dst] =
                      [ @ EXCEPT !.kv_store = newKVStore,
                                 !.client_table = @ \cup { m.requestID } ]]
  /\ UNCHANGED << switchState, requestID >>

\* Destination server handles read m
HandleRead(m) ==
LET
  s == serverState[m.dst]
IN
  /\ Send([ mtype          |-> MReadReply,
            key            |-> m.key,
            sender         |-> m.dst,
            ver            |-> s.kv_store[m.key],
            ghostLastReply |-> m.ghostLastReply ])
  /\ UNCHANGED << switchState, serverState, requestID >>

\* Switch handles WriteReply m
HandleReply(m) ==
LET
  isReplicated == m.key \in switchState.rkeys
  isNew == m.ver > switchState.ver_completed[m.key]
  isCurrent == m.ver = switchState.ver_completed[m.key]
  newKeyRset == CASE isNew     -> { m.sender }
                  [] isCurrent -> switchState.rset[m.key] \cup { m.sender }
                  [] OTHER     -> switchState.rset[m.key]
  newRset == IF isReplicated
             THEN (m.key :> newKeyRset) @@ switchState.rset
             ELSE switchState.rset
  newVerCompleted == IF isReplicated /\ isNew
                     THEN (m.key :> m.ver) @@ switchState.ver_completed
                     ELSE switchState.ver_completed
IN
  /\ switchState' = [ switchState EXCEPT !.ver_completed = newVerCompleted,
                                         !.rset = newRset ]
  /\ \/ /\ m.mtype = MWriteReply
        /\ Send([ mtype     |-> MClientWriteReply,
                  key       |-> m.key,
                  requestID |-> m.requestID,
                  ghostVer  |-> m.ver ])
     \/ /\ m.mtype = MReadReply
        /\ Send([ mtype          |-> MClientReadReply,
                  key            |-> m.key,
                  ver            |-> m.ver,
                  ghostLastReply |-> m.ghostLastReply ])
  /\ UNCHANGED << serverState, requestID >>

TransferState(s1, s2) ==
LET
  newKVStore == [ k \in keys |->
    CASE serverState[s1].kv_store[k] > serverState[s2].kv_store[k]
           -> serverState[s1].kv_store[k]
      [] OTHER -> serverState[s2].kv_store[k] ]
IN
  /\ serverState' = [ serverState EXCEPT ![s2] =
                      [ @ EXCEPT !.client_table = @ \cup serverState[s1].client_table,
                                 !.kv_store = newKVStore ]]
  /\ UNCHANGED << messages, switchState, requestID >>

\* This specification models mode switching as an atomic action for simplicity
SwitchModes(k) ==
\E s \in servers :
LET
  hn == HomeNode(k, s)[k]
IN
  /\ \/ /\ k \in switchState.rkeys
        \* Check that home node is up to date
        /\ \A sp \in servers :
           /\ serverState[sp].client_table \subseteq serverState[hn].client_table
           /\ serverState[sp].kv_store[k] <= serverState[hn].kv_store[k]
        /\ switchState' = [ switchState EXCEPT !.rkeys = @ \ { k },
                                               !.home_node = HomeNode(k, s) ]
        /\ serverState' = [ a \in servers |->
                            [ serverState[a] EXCEPT !.rkeys = @ \ { k } ]]
     \/ /\ k \notin switchState.rkeys
        \* Check that the client table has been properly propagated
        /\ \A sp \in servers :
           /\ serverState[hn].client_table \subseteq serverState[sp].client_table
        /\ switchState' = [ switchState EXCEPT !.rkeys = @ \cup { k },
                                               !.rset = (k :> { hn }) @@ @,
                                               !.home_node = HomeNode(k, s) ]
        /\ serverState' = [ a \in servers |->
                            [ serverState[a] EXCEPT !.rkeys = @ \cup { k } ]]
  /\ UNCHANGED << messages, requestID >>


--------------------------------------------------------------------------------
(* `^\textbf{\large Main Transition Function}^' *)

Next == \/ \E k \in keys : \/ SendClientWrite(k)
                           \/ SendRead(k)
                           \/ SwitchModes(k)
        \/ \E m \in messages : \/ /\ m.mtype = MClientWrite
                                  /\ HandleClientWrite(m)
                               \/ /\ m.mtype = MWrite
                                  /\ HandleWrite(m)
                               \/ /\ m.mtype = MRead
                                  /\ HandleRead(m)
                               \/ /\ m.mtype = MWriteReply
                                  /\ HandleReply(m)
                               \/ /\ m.mtype = MReadReply
                                  /\ HandleReply(m)
        \/ \E s1 \in servers : \E s2 \in servers : TransferState(s1, s2)


================================================================================
