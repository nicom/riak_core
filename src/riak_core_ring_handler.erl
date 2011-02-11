%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.

%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.

-module(riak_core_ring_handler).
-behaviour(gen_event).

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2,
         handle_info/2, terminate/2, code_change/3]).
-record(state, {}).


%% ===================================================================
%% gen_event callbacks
%% ===================================================================

init([]) ->
    %% Pull the initial ring and make sure all vnodes are started
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    ensure_vnodes_started(Ring),
    {ok, #state{}}.

handle_event({ring_update, Ring}, State) ->
    %% Make sure all vnodes are started...
    ensure_vnodes_started(Ring),
    {ok, State}.

handle_call(_Event, State) ->
    {ok, ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



%% ===================================================================
%% Internal functions
%% ===================================================================

ensure_vnodes_started(Ring) ->
    case riak_core:vnode_modules() of
        [] -> ok;
        Mods ->
            case ensure_vnodes_started(Mods, Ring, []) of
                [] -> riak_core:stop("node removal completed, exiting.");
                _ -> ok
            end
    end.

ensure_vnodes_started([], _Ring, Acc) ->
    lists:flatten(Acc);
ensure_vnodes_started([H|T], Ring, Acc) ->
    ensure_vnodes_started(T, Ring, [ensure_vnodes_started(H, Ring)|Acc]).


pforeach(F, L) ->
    S = self(),
    Ref = make_ref(),
    Parts = partition_list(L, length(L) div 4, length(L) rem 4, []),
    Pids = lists:map(fun(I) -> spawn(fun() -> pforeach_f(Ref, S, F, I) end) end, Parts),
    pforeach_gather(Ref, Pids).

partition_list([], _, _, Acc) ->
   Acc;
partition_list(L, PartLen, 0, Acc) ->
   {Part, Rest} = lists:split(PartLen, L),
   partition_list(Rest, PartLen, 0, [Part|Acc]);
partition_list(L, PartLen, Remainder, Acc) ->
   {Part, Rest} = lists:split(PartLen+1, L),
   partition_list(Rest, PartLen, Remainder-1, [Part|Acc]).

pforeach_gather(Ref, [H|T]) ->
   receive
     {Ref, H} -> pforeach_gather(Ref, T)
   end;
pforeach_gather(_Ref, []) ->
   [].
   
pforeach_f(Ref, Parent, F, I) ->
    _ = (catch lists:foreach(F,I)),
    Parent ! {Ref, self()}.
    

ensure_vnodes_started(Mod, Ring) ->
    Startable = startable_vnodes(Mod, Ring),
    pforeach(fun(E) -> error_logger:info_msg("Starting Vnode ~p",[E]), Mod:start_vnode(E) end, Startable),
    Startable.

startable_vnodes(Mod, Ring) ->
    AllMembers = riak_core_ring:all_members(Ring),
    case {length(AllMembers), hd(AllMembers) =:= node()} of
        {1, true} ->
            riak_core_ring:my_indices(Ring);
        _ ->
            {ok, Excl} = riak_core_handoff_manager:get_exclusions(Mod),
            case riak_core_ring:random_other_index(Ring, Excl) of
                no_indices ->
                    case length(Excl) =:= riak_core_ring:num_partitions(Ring) of
                        true ->
                            [];
                        false ->
                            riak_core_ring:my_indices(Ring)
                    end;
                RO ->
                    [RO | riak_core_ring:my_indices(Ring)]
            end
    end.
