-module(aggr_suite).

-include_lib("eunit/include/eunit.hrl").
-include("mstore.hrl").

listavg_test() ->
    ?assertEqual([1.0], listavg([1],1)).

empty_test() ->
    ?assertEqual(<<>>, mstore_bin:empty(0)),
    ?assertEqual(<<0:(?BITS+8)/signed-integer>>, mstore_bin:empty(1)),
    ?assertEqual(<<0:((?BITS+8)*2)/signed-integer>>, mstore_bin:empty(2)).

find_type_test() ->
    T1 = <<?NONE, 0:?BITS/signed-integer, ?INT, 0:?BITS/signed-integer>>,
    T2 = <<?NONE, 0:?BITS/signed-integer, ?FLOAT, 0:?BITS/float>>,
    T3 = <<?NONE, 0:?BITS/signed-integer, ?NONE, 0:?BITS/signed-integer>>,
    T4 = <<?NONE, 0:?BITS/signed-integer, ?NONE>>,
    ?assertEqual(integer, mstore_bin:find_type(T1)),
    ?assertEqual(float, mstore_bin:find_type(T2)),
    ?assertEqual(unknown, mstore_bin:find_type(T3)),
    ?assertEqual(unknown, mstore_bin:find_type(T4)).

avg_test() ->
    ?assertEqual([1.0,2.0,3.0], mstore_bin:to_list(mstore_aggr:avg(mstore_bin:from_list([1,2,3]), 1))),
    ?assertEqual([1.0,2.0,3.0], mstore_bin:to_list(mstore_aggr:avg(mstore_bin:from_list([1,1,2,2,3,3]), 2))),
    ?assertEqual([2.0,3.0,4.0], mstore_bin:to_list(mstore_aggr:avg(mstore_bin:from_list([1,3,2,4,3,5]), 2))),
    ?assertEqual([2.0,3.0,4.0,2.0], mstore_bin:to_list(mstore_aggr:avg(mstore_bin:from_list([1,3,2,4,3,5,4]), 2))).

sum_test() ->
    ?assertEqual([1,2,3], mstore_bin:to_list(mstore_aggr:sum(mstore_bin:from_list([1,2,3]), 1))),
    ?assertEqual([2,4,6], mstore_bin:to_list(mstore_aggr:sum(mstore_bin:from_list([1,1,2,2,3,3]), 2))),
    ?assertEqual([4,6,8], mstore_bin:to_list(mstore_aggr:sum(mstore_bin:from_list([1,3,2,4,3,5]), 2))),
    ?assertEqual([4,6,8,4], mstore_bin:to_list(mstore_aggr:sum(mstore_bin:from_list([1,3,2,4,3,5,4]), 2))),
    %% We use the average of 1 field to convert the data to floats, this tests
    %% if mstore_aggr:summing of floats works too.
    ?assertEqual([1.0,2.0,3.0], mstore_bin:to_list(mstore_aggr:sum(mstore_aggr:avg(mstore_bin:from_list([1,2,3]),1), 1))),
    ?assertEqual([2.0,4.0,6.0], mstore_bin:to_list(mstore_aggr:sum(mstore_aggr:avg(mstore_bin:from_list([1,1,2,2,3,3]), 1), 2))),
    ?assertEqual([4.0,6.0,8.0], mstore_bin:to_list(mstore_aggr:sum(mstore_aggr:avg(mstore_bin:from_list([1,3,2,4,3,5]), 1), 2))),
    ?assertEqual([4.0,6.0,8.0,4.0], mstore_bin:to_list(mstore_aggr:sum(mstore_aggr:avg(mstore_bin:from_list([1,3,2,4,3,5,4]), 1), 2))).

derivate_test() ->
    ?assertEqual([1,1], mstore_bin:to_list(mstore_aggr:derivate(mstore_bin:from_list([1,2,3])))).

proper_module_test() ->
    ?assertEqual([], proper:module(aggr_proper, [long_result, {to_file, user}, {numtests, 300}])).

-ifdef(BENCH).
bench_test() ->
    Num = 1000000,
    AvgSize = 10,
    ListData = lists:seq(0,Num),
    {T1, BinData} = timer:tc(fun() ->
                                     mstore_bin:from_list(ListData)
                             end),
    Seconds1 = T1 / 1000000,
    ?debugFmt("Converted ~p points in ~p seconds meaning ~p points/second.",
              [Num, Seconds1, Num/Seconds1]),

    {T2, AvgL1} = timer:tc(fun() ->
                                   L = mstore_bin:to_list(BinData),
                                   L1 = listavg(L, AvgSize),
                                   mstore_bin:from_list(L1)
                           end),
    Seconds2 = T2 / 1000000,
    ?debugFmt("Calculated mstore_aggr:avg (~p) of ~p points in ~p seconds meaning ~p points/second from a bin->list->bin.",
              [Num, Seconds2, AvgSize, Num/Seconds2]),

    {T3, AvgB} = timer:tc(fun() ->
                                  mstore_aggr:avg(BinData, AvgSize)
                          end),
    Seconds3 = T3 / 1000000,
    ?debugFmt("Calculated mstore_aggr:avg (~p) of ~p points in ~p seconds meaning ~p points/second from a binary.",
              [Num, Seconds3, AvgSize, Num/Seconds3]),
    {T4, AvgL2} = timer:tc(fun() ->
                                   listavg(ListData, AvgSize)
                           end),
    Seconds4 = T4 / 1000000,
    ?debugFmt("Calculated mstore_aggr:avg (~p) of ~p points in ~p seconds meaning ~p points/second from a list_only.",
              [Num, Seconds4, AvgSize, Num/Seconds4]),
    ?assertEqual(AvgB, AvgL1),
    ?assertEqual(AvgB, mstore_bin:from_list(AvgL2)).
-endif.

listavg(Data, Count) ->
    listavg(Data, 0, Count, Count, []).

listavg(R, Sum, 0, Count, Acc) ->
    Avg = Sum/Count,
    Acc1 = [Avg | Acc],
    listavg(R, 0, Count, Count, Acc1);

listavg([I | R], Sum, N, Count, Acc) ->
    listavg(R, Sum+I, N-1, Count, Acc);

listavg([], 0, _Count, _Count, Acc) ->
    lists:reverse(Acc);

listavg([], Sum, _Missing, Count, Acc) ->
    Avg = Sum/Count,
    lists:reverse([Avg | Acc]).
