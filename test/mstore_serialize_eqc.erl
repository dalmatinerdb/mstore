-module(mstore_serialize_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("../include/mstore.hrl").

-import(mstore_heler, [int_array/0, float_array/0, pos_int/0, non_neg_int/0,
                       i_or_f_list/0, i_or_f_array/0, non_empty_i_or_f_list/0]).

-compile(export_all).

-define(S, mstore).
-define(G, gb_trees).
-define(M, "metric").
-define(DIR, ".qcdata").

store(Buckets, FileSize) ->
    ?SIZED(Size,store(Buckets, FileSize, Size)).

store(Buckets, FileSize, Size) ->
    ?LAZY(oneof([{call,?MODULE, new, [Buckets, FileSize, ?DIR]}]
                ++ [{call, ?S, put, [store(Buckets, FileSize, Size-1), metric_name(), offset(), non_z_int()]} || Size > 0])).

new(NumFiles, FileSize, Dir) ->
    {ok, MSet} = mstore:new(NumFiles, FileSize, Dir),
    MSet.

non_z_int() ->
    ?SUCHTHAT(I, int(), I =/= 0).

metric_name() ->
    ?SUCHTHAT(L, list(choose($a, $z)), L =/= "").

chash_size() ->
    ?LET(N, choose(1, 5), trunc(math:pow(2, N))).

size() ->
    choose(1000,2000).
offset() ->
    choose(0, 5000).

mset_serializer(S) ->
    receive
        {From, Ref, get} ->
            From ! {Ref, S},
            mset_serializer(S);
        {put, M, T, V} ->
            S1 = ?S:put(S, M, T, V),
            mset_serializer(S1)
    end.

prop_fold_fully() ->
    ?FORALL({NumFiles, FileSize}, {chash_size(), size()},
            ?FORALL(D, store(NumFiles, FileSize),
                    begin
                        os:cmd("rm -r " ++ ?DIR ++"-copy"),
                        os:cmd("rm -r " ++ ?DIR),
                        Original = eval(D),
                        Copy = new(NumFiles, FileSize, ?DIR ++"-copy"),
                        OriginalSetSer = spawn(?MODULE, mset_serializer, [Copy]),
                        SerializeOrig = fun(M, T, V, Acc) ->
                                                OriginalSetSer ! {put, M, T, V},
                                                [{M, T, V} | Acc]
                                        end,
                        SerializeCopy = fun(M, T, V, Acc) ->
                                                [{M, T, V} | Acc]
                                        end,
                        L1 = ?S:fold(Original, SerializeOrig, []),
                        R0 = make_ref(),
                        OriginalSetSer ! {self(), R0, get},
                        C1 = receive
                                 {R0, Cr} ->
                                     Cr
                             after 1000 ->
                                     error
                             end,
                        L2 = ?S:fold(C1, SerializeCopy, []),
                        lists:sort(L1) == lists:sort(L2)
                    end)).

-include("eqc_helper.hrl").
