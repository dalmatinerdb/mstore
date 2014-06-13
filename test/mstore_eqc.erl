-module(mstore_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("../include/mstore.hrl").

-import(mstore_heler, [int_array/0, float_array/0, pos_int/0, non_neg_int/0,
                       i_or_f_list/0, i_or_f_array/0, non_empty_i_or_f_list/0]).

-record(state, {size, data}).

-compile(export_all).
%%%-------------------------------------------------------------------
%%% Generators
%%%-------------------------------------------------------------------

-define(S, mstore).
-define(G, gb_trees).
-define(M, "metric").
-define(DIR, ".qcdata").

store(Buckets, FileSize) ->
    ?SIZED(Size,store(Buckets, FileSize, Size)).

insert(Buckets, FileSize, Size) ->
    ?LAZY(?LET({{S, T}, M, O, V},
               {store(Buckets, FileSize, Size-1), ?M, offset(), non_z_int()},
               {{call, ?S, put, [S, M, O, V]},
                {call, ?G, enter, [O, V, T]}})).

reopen(Buckets, FileSize, Size) ->
    ?LAZY(?LET({S, T},
               store(Buckets, FileSize, Size-1),
               {oneof(
                  [{call,?MODULE,renew, [S,Buckets, FileSize, ?DIR]},
                   {call,?MODULE,reopen, [S,?DIR]}]), T})).

store(Buckets, FileSize, Size) ->
    ?LAZY(oneof([{{call,?MODULE,new, [Buckets, FileSize, ?DIR]}, {call, ?G, empty, []}}]
                ++ [frequency(
                      [{9, insert(Buckets, FileSize, Size)},
                       {1, reopen(Buckets, FileSize, Size)}]) || Size > 0])).

reopen(Old, Dir) ->
    ok = mstore:close(Old),
    {ok, MSet} = mstore:open(Dir),
    MSet.

renew(Old, NumFiles, FileSize, Dir) ->
    ok = mstore:close(Old),
    new(NumFiles, FileSize, Dir).

new(NumFiles, FileSize, Dir) ->
    {ok, MSet} = mstore:new(NumFiles, FileSize, Dir),
    MSet.


non_z_int() ->
    ?SUCHTHAT(I, int(), I =/= 0).

chash_size() ->
    ?LET(N, choose(1, 5), trunc(math:pow(2, N))).

size() ->
    choose(1000,2000).

offset() ->
    choose(0, 5000).


string() ->
    ?SUCHTHAT(L, list(char()), L =/= "").

unlist([E]) ->
    E.
%%%-------------------------------------------------------------------
%%% Properties
%%%-------------------------------------------------------------------
prop_read_write() ->
    os:cmd("rm -r " ++ ?DIR),
    ?FORALL({Metric, Size, Time, Data},
            {string(), size(), offset(), non_empty_i_or_f_list()},
            begin
                {ok, S1} = ?S:new(2, Size, ?DIR),
                S2 = ?S:put(S1, Metric, Time, Data),
                {ok, Res1} = ?S:get(S2, Metric, Time, length(Data)),
                Res2 = mstore_bin:to_list(Res1),
                Metrics = ?S:metrics(S2),
                ?S:delete(S2),
                Res2 == Data andalso
                    Metrics == [Metric]
            end).

prop_gb_comp() ->
    os:cmd("rm -r " ++ ?DIR),
    ?FORALL({Buckets, FileSize}, {chash_size(), size()},
            ?FORALL(D, store(Buckets, FileSize),
                    begin
                        {S, T} = eval(D),
                        List = ?G:to_list(T),
                        List1 = [{?S:get(S, ?M, Time, 1), V} || {Time, V} <- List],
                        ?S:delete(S),
                        List2 = [{unlist(mstore_bin:to_list(Vs)), Vt} || {{ok, Vs}, Vt} <- List1],
                        List3 = [true || {_V, _V} <- List2],
                        Len = length(List),
                        length(List1) == Len andalso
                            length(List2) == Len andalso
                            length(List3) == Len
                    end
                    )).
-include("eqc_helper.hrl").
