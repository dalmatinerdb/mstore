%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created :  8 Jun 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(mstore_aggr).

-export([sum/2, avg/2, derivate/1]).
-include("mstore.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

avg(Data, Count) ->
    avg(Data, 0, Count, Count, <<>>).

avg(R, Sum, 0, Count, Acc) ->
    Avg = Sum/Count,
    Acc1 = <<Acc/binary, ?FLOAT, Avg:?BITS/float>>,
    avg(R, 0, Count, Count, Acc1);
avg(<<?INT, I:?BITS/integer, R/binary>>, Sum, N, Count, Acc) ->
    avg(R, Sum+I, N-1, Count, Acc);

avg(<<?FLOAT, I:?BITS/float, R/binary>>, Sum, N, Count, Acc) ->
    avg(R, Sum+I, N-1, Count, Acc);

avg(<<?NONE, 0:?BITS/float, R/binary>>, Sum, N, Count, Acc) ->
    avg(R, Sum, N-1, Count, Acc);

avg(<<>>, 0, _Count, _Count, Acc) ->
    Acc;

avg(<<>>, Sum, _Missing, Count, Acc) ->
    Avg = Sum/Count,
    <<Acc/binary, ?FLOAT, Avg:?BITS/float>>;

avg(A, B, C, D, E) ->
    io:format("avg(~p, ~p, ~p, ~p, ~p)~n", [A, B, C, D, E]).

sum(<<?INT, _/binary>> = Data, Count) ->
    sum_int(Data, 0, Count, Count, <<>>);

sum(<<?FLOAT, _/binary>> = Data, Count) ->
    sum_float(Data, 0, Count, Count, <<>>);

sum(Data, Count) ->
    case find_type(Data) of
        integer ->
            sum_int(Data, 0, Count, Count, <<>>);
        float ->
            sum_int(Data, 0, Count, Count, <<>>);
        unknown ->
            empty(byte_size(Data)/?DATA_SIZE)
    end.

sum_float(R, Sum, 0, Count, Acc) ->
    Acc1 = <<Acc/binary, ?FLOAT, Sum:?BITS/float>>,
    sum_float(R, 0, Count, Count, Acc1);
sum_float(<<?FLOAT, I:?BITS/float, R/binary>>, Sum, N, Count, Acc) ->
    sum_float(R, Sum+I, N-1, Count, Acc);
sum_float(<<?NONE, 0:?BITS/float, R/binary>>, Sum, N, Count, Acc) ->
    sum_float(R, Sum, N-1, Count, Acc);
sum_float(<<>>, 0, _Count, _Count, Acc) ->
    Acc;
sum_float(<<>>, Sum, _, _, Acc) ->
    <<Acc/binary, ?FLOAT, Sum:?BITS/float>>.

sum_int(R, Sum, 0, Count, Acc) ->
    Acc1 = <<Acc/binary, ?INT, Sum:?BITS/integer>>,
    sum_int(R, 0, Count, Count, Acc1);
sum_int(<<?INT, I:?BITS/integer, R/binary>>, Sum, N, Count, Acc) ->
    sum_int(R, Sum+I, N-1, Count, Acc);
sum_int(<<?NONE, 0:?BITS/integer, R/binary>>, Sum, N, Count, Acc) ->
    sum_int(R, Sum, N-1, Count, Acc);
sum_int(<<>>, 0, _Count, _Count, Acc) ->
    Acc;
sum_int(<<>>, Sum, _, _, Acc) ->
    <<Acc/binary, ?INT, Sum:?BITS/integer>>.

derivate(<<?INT, I:?BITS/integer, Rest/binary>>) ->
    der_int(Rest, I, <<>>);

derivate(<<?FLOAT, I:?BITS/float, Rest/binary>>) ->
    der_float(Rest, I, <<>>).

der_int(<<?INT, I:?BITS/integer, Rest/binary>>, Last, Acc) ->
    der_int(Rest, I, <<Acc/binary, ?INT, (I - Last):?BITS/integer>>);
der_int(<<?NONE, 0:?BITS/integer, Rest/binary>>, Last, Acc) ->
    der_int(Rest, Last, <<Acc/binary, ?INT, 0:?BITS/integer>>);
der_int(<<>>, _, Acc) ->
    Acc.

der_float(<<?FLOAT, I:?BITS/float, Rest/binary>>, Last, Acc) ->
    der_float(Rest, I, <<Acc/binary, ?FLOAT, (I - Last):?BITS/float>>);
der_float(<<?NONE, 0:?BITS/float, Rest/binary>>, Last, Acc) ->
    der_float(Rest, Last, <<Acc/binary, ?FLOAT, 0:?BITS/float>>);
der_float(<<>>, _, Acc) ->
    Acc.


find_type(<<?INT, _:?BITS/integer, _/binary>>) ->
    integer;
find_type(<<?FLOAT, _:?BITS/float, _/binary>>) ->
    float;
find_type(<<?NONE, _:?BITS/integer, Rest/binary>>) ->
    find_type(Rest);
find_type(_) ->
    unknown.

empty(Length) ->
    <<0:((?BITS+8)*Length)/integer>>.

-ifdef(TEST).
to_bin(L) ->
    << <<?INT, V:?BITS/integer>> || V <- L >>.

empty_test() ->
    ?assertEqual(<<>>, empty(0)),
    ?assertEqual(<<0:(?BITS+8)/integer>>, empty(1)),
    ?assertEqual(<<0:((?BITS+8)*2)/integer>>, empty(2)).

find_type_test() ->
    T1 = <<?NONE, 0:?BITS/integer, ?INT, 0:?BITS/integer>>,
    T2 = <<?NONE, 0:?BITS/integer, ?FLOAT, 0:?BITS/float>>,
    T3 = <<?NONE, 0:?BITS/integer, ?NONE, 0:?BITS/integer>>,
    T4 = <<?NONE, 0:?BITS/integer, ?NONE>>,
    ?assertEqual(integer, find_type(T1)),
    ?assertEqual(float, find_type(T2)),
    ?assertEqual(unknown, find_type(T3)),
    ?assertEqual(unknown, find_type(T4)).

avg_test() ->
    ?assertEqual([1.0,2.0,3.0], mstore:to_list(avg(to_bin([1,2,3]), 1))),
    ?assertEqual([1.0,2.0,3.0], mstore:to_list(avg(to_bin([1,1,2,2,3,3]), 2))),
    ?assertEqual([2.0,3.0,4.0], mstore:to_list(avg(to_bin([1,3,2,4,3,5]), 2))),
    ?assertEqual([2.0,3.0,4.0,2.0], mstore:to_list(avg(to_bin([1,3,2,4,3,5,4]), 2))).

sum_test() ->
    ?assertEqual([1,2,3], mstore:to_list(sum(to_bin([1,2,3]), 1))),
    ?assertEqual([2,4,6], mstore:to_list(sum(to_bin([1,1,2,2,3,3]), 2))),
    ?assertEqual([4,6,8], mstore:to_list(sum(to_bin([1,3,2,4,3,5]), 2))),
    ?assertEqual([4,6,8,4], mstore:to_list(sum(to_bin([1,3,2,4,3,5,4]), 2))),
    %% We use the average of 1 field to convert the data to floats, this tests
    %% if summing of floats works too.
    ?assertEqual([1.0,2.0,3.0], mstore:to_list(sum(avg(to_bin([1,2,3]),1), 1))),
    ?assertEqual([2.0,4.0,6.0], mstore:to_list(sum(avg(to_bin([1,1,2,2,3,3]), 1), 2))),
    ?assertEqual([4.0,6.0,8.0], mstore:to_list(sum(avg(to_bin([1,3,2,4,3,5]), 1), 2))),
    ?assertEqual([4.0,6.0,8.0,4.0], mstore:to_list(sum(avg(to_bin([1,3,2,4,3,5,4]), 1), 2))).

derivate_test() ->
    ?assertEqual([1,1], mstore:to_list(derivate(to_bin([1,2,3])))).


-endif.
