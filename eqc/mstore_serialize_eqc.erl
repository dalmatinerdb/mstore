-module(mstore_serialize_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("../include/mstore.hrl").

-import(mstore_heler, [int_array/0, pos_int/0, non_neg_int/0,
                        non_empty_int_list/0, defined_int_array/0]).

-export([prop_fold_fully/0,
         prop_count/0]).

-define(S, mstore).
-define(G, gb_trees).
-define(M, "metric").
-define(DIR, ".qcdata").

store(FileSize) ->
    ?SIZED(Size, store(FileSize, Size)).

store(FileSize, Size) ->
    ?LAZY(oneof([{call, ?MODULE, new, [FileSize, ?DIR]} || Size == 0]
                ++ [{call, ?MODULE, reopen, [store(FileSize, Size-1), FileSize, ?DIR]}  || Size > 0]
                ++ [{call, ?MODULE, reindex, [store(FileSize, Size-1)]}  || Size > 0]
                ++ [{call, ?S, put, [store(FileSize, Size-1), metric_name(), offset(), non_z_int()]} || Size > 0])).

new(FileSize, Dir) ->
    {ok, MSet} = mstore:new(Dir, [{file_size, FileSize}]),
    MSet.

reopen(Store, FileSize, Dir) ->
    mstore:close(Store),
    {ok, MSet} = mstore:new(Dir, [{file_size, FileSize}]),
    MSet.
reindex(Store) ->
    {ok, MSet} = mstore:reindex(Store),
    MSet.

non_z_int() ->
    ?SUCHTHAT(I, int(), I =/= 0).

metric_name() ->
    ?LET(S, ?SUCHTHAT(L, list(choose($a, $z)), L =/= ""), list_to_binary(S)).

chash_size() ->
    ?LET(N, choose(1, 5), trunc(math:pow(2, N))).

size() ->
    choose(1000,2000).

offset() ->
    choose(0, 5000).

chunk() ->
    oneof([choose(0, 100), infinity]).

mset_serializer(S) ->
    receive
        {From, Ref, get} ->
            From ! {Ref, S};
        {put, M, T, V} ->
            S1 = ?S:put(S, M, T, V),
            mset_serializer(S1)
    end.
prop_fold_fully() ->
    ?FORALL(FileSize, size(),
            ?FORALL({Chunk, D}, {chunk(), store(FileSize)},
                    begin
                        os:cmd("rm -r " ++ ?DIR ++"-copy"),
                        os:cmd("rm -r " ++ ?DIR),
                        Original = eval(D),
                        Copy = new(FileSize, ?DIR ++"-copy"),
                        OriginalSetSer = spawn(?MODULE, mset_serializer, [Copy]),
                        SerializeOrig = fun(M, T, V, Acc) ->
                                                OriginalSetSer ! {put, M, T, V},
                                                [{M, T, V} | Acc]
                                        end,
                        SerializeCopy = fun(M, T, V, Acc) ->
                                                [{M, T, V} | Acc]
                                        end,
                        L1 = ?S:fold(Original, SerializeOrig, Chunk, []),
                        R0 = make_ref(),
                        OriginalSetSer ! {self(), R0, get},
                        C1 = receive
                                 {R0, Cr} ->
                                     Cr
                             after 1000 ->
                                     error
                             end,
                        L2 = ?S:fold(C1, SerializeCopy, []),
                        ?S:close(Original),
                        ?S:close(C1),
                        lists:sort(L1) == lists:sort(L2)
                    end)).


prop_count() ->
    ?FORALL(FileSize, size(),
            ?FORALL(D, store(FileSize),
                    begin
                        os:cmd("rm -r " ++ ?DIR ++"-copy"),
                        os:cmd("rm -r " ++ ?DIR),
                        Store = eval(D),
                        Count = mstore:count(Store),
                        FoldFn = fun(M, T, _, {M, F, Cnt})
                                    when T div FileSize == F->
                                         {M, F, Cnt};
                                    (M, T, _, {_, _, Cnt}) ->
                                         {M, T div FileSize, Cnt + 1}
                                 end,
                        {_, _, Count2} = ?S:fold(Store, FoldFn, {undefined, 0, 0}),
                        ?S:close(Store),
                        ?WHENFAIL(io:format(user, "cont():~p /= fold():~p~n",
                                            [Count, Count2]),
                                  Count == Count2)
                    end)).
