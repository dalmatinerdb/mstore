-module(aggr_proper).
-include_lib("proper/include/proper.hrl").
-include("mstore.hrl").


int_array() ->
    ?LET(L, list({frequency([{2, false}, {8, true}]), integer()}),
         {L, to_list(L, 0, []), to_bin(L, <<>>)}).

float_array() ->
    ?LET(L, list({frequency([{2, false}, {8, true}]), float()}),
         {L, to_list_f(L), to_bin(L, <<>>)}).

to_list_f(L) ->
    case lists:keyfind(true, 1, L) of
        false ->
            to_list(L, 0, []);
        _ ->
            to_list(L, 0.0, [])
    end.

to_list([{false, _} | R], Last, Acc) ->
    to_list(R, Last, [Last | Acc]);
to_list([{true, V} | R], _, Acc) ->
    to_list(R, V, [V | Acc]);
to_list([], _, Acc) ->
    lists:reverse(Acc).

to_bin([{false, _} | R], Acc) ->
    to_bin(R, <<Acc/binary, ?NONE, 0:?BITS/signed-integer>>);

to_bin([{true, V} | R], Acc) when is_integer(V) ->
    to_bin(R, <<Acc/binary, ?INT, V:?BITS/signed-integer>>);

to_bin([{true, V} | R], Acc) when is_float(V) ->
    to_bin(R, <<Acc/binary, ?FLOAT, V:?BITS/float>>);

to_bin([], Acc) ->
    Acc.

prop_l2b_b2l() ->
    ?FORALL(T, list(integer()),
            T =:= mstore_bin:to_list(mstore_bin:from_list(T))),
    ?FORALL(T, list(float()),
            T =:= mstore_bin:to_list(mstore_bin:from_list(T))).

prop_b2l() ->
    ?FORALL({_, L, B}, int_array(),
            L =:= mstore_bin:to_list(B)),
    ?FORALL({_, L, B}, float_array(),
            L =:= mstore_bin:to_list(B)).


prop_n_length_chunks() ->
    ?FORALL({L, N}, {list(integer()), pos_integer()},
            ceiling(length(L) / N) =:= length(n_length_chunks(L, N))).

prop_avg_int() ->
    ?FORALL({{_, L, B}, N}, {int_array(), pos_integer()},
            begin
                SL = avg(L, N),
                SB = mstore_bin:to_list(mstore_aggr:avg(B, N)),
                ?WHENFAIL(io:format("~p =/= ~p", [SL, SB]),
                          SL =:= SB)
            end).

prop_avg_float() ->
    ?FORALL({{_, L, B}, N}, {float_array(), pos_integer()},
            begin
                SL = avg(L, N),
                SB = mstore_bin:to_list(mstore_aggr:avg(B, N)),
                ?WHENFAIL(io:format("~p =/= ~p", [SL, SB]),
                          SL =:= SB)
            end).

prop_sum_int() ->
    ?FORALL({{_, L, B}, N}, {int_array(), pos_integer()},
            begin
                SL = sum(L, N),
                SB = mstore_bin:to_list(mstore_aggr:sum(B, N)),
                ?WHENFAIL(io:format("~p =/= ~p", [SL, SB]),
                          SL =:= SB)
            end).

prop_sum_float() ->
    ?FORALL({{_, L, B}, N}, {float_array(), pos_integer()},
            begin
                SL = sum(L, N),
                SB = mstore_bin:to_list(mstore_aggr:sum(B, N)),
                ?WHENFAIL(io:format("~p =/= ~p", [SL, SB]),
                          SL =:= SB)
            end).

prop_dev_int() ->
    ?FORALL({_, L, B}, int_array(),
            begin
                DL = derivate(L),
                DB = mstore_bin:to_list(mstore_aggr:derivate(B)),
                ?WHENFAIL(io:format("~p =/= ~p", [DL, DB]),
                          DL =:= DB)
            end).

prop_dev_float() ->
    ?FORALL({_, L, B}, float_array(),
            begin
                DL = derivate(L),
                DB = mstore_bin:to_list(mstore_aggr:derivate(B)),
                ?WHENFAIL(io:format("~p =/= ~p", [DL, DB]),
                          DL =:= DB)
            end).

avg(L, N) ->
    apply_n(L, N, fun avg_/2).

sum(L, N) ->
    apply_n(L, N, fun sum_/2).

apply_n(L, N, F) ->
    [F(SL, N) || SL <- n_length_chunks(L, N)].

avg_(L, N) ->
    lists:sum(L) / N.

sum_(L, _N) ->
    lists:sum(L).

derivate([]) ->
    [];
derivate([H | T]) ->
    derivate(H, T, []).
derivate(H, [H1 | T], Acc) ->
    derivate(H1, T, [H1 - H | Acc]);
derivate(_, [], Acc) ->
    lists:reverse(Acc).

%% Taken from: http://schemecookbook.org/Erlang/NumberRounding
ceiling(X) ->
    T = erlang:trunc(X),
    case (X - T) of
        Neg when Neg < 0 -> T;
        Pos when Pos > 0 -> T + 1;
        _ -> T
    end.
%% taken from http://stackoverflow.com/questions/12534898/splitting-a-list-in-equal-sized-chunks-in-erlang
n_length_chunks([],_) -> [];
n_length_chunks(List,Len) when Len > length(List) ->
    [List];
n_length_chunks(List,Len) ->
    {Head,Tail} = lists:split(Len,List),
    [Head | n_length_chunks(Tail,Len)].
